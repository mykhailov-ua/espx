# eSPX Architecture Specification

## Topology

The pipeline consists of five layers:

1. **Ingress (Nginx)**: Edge load balancer. Routes `/admin/*` to the Control Plane and `/track/*` to the Ingestion Plane.
2. **Control Plane**:
   - **Management Gateway (`:8188`)**: Exposes REST interfaces for campaign management, RBAC verification, and outbox event logging.
   - **Auth Service (`:51051`)**: Internal gRPC microservice for Argon2id hashing and PASETO token generation.
3. **Ingestion Plane (Trackers)**:
   - Stateless Go instances (`:8181-8184`) running in host networking mode.
   - Event-driven I/O via `gnet/v2` with 2 event loops per instance. OS thread locking is disabled (`gnet.WithLockOSThread(false)`).
   - Task dispatch to workers (`PinnedWorkerPool`) using lock-free MPSC ring buffers.
   - Zero-allocation connection-local pool (`connContext`) bound to connection lifetime.
   - Zero-copy DFA HTTP/1.1 stream parser mapping headers directly from socket ring buffers.
4. **Caching State (Redis)**:
   - 6-node Redis cluster sharded via client-side `StaticSlotSharder` using O(1) constant-time lookup.
   - Executes atomic Lua scripts for budget allocation, pacing checks, and user frequency capping.
   - Ingress load balancer caches blacklist check results in a local shared dictionary (`blacklist_cache`) with a 300-second TTL. Client IP hashing (`ngx.crc32_long`) is used to select the Redis shard.
5. **Settlement & Storage**:
   - **Processor Pool (`:8186`)**: Background consumer workers fetching stream batches from Redis Consumer Groups.
   - **PostgreSQL 16**: Relational storage for accounts, ledgers, audit logs, and daily partition tables.
   - **ClickHouse**: Columnar data warehouse for event telemetry. If batching is active (batch size > 1), events are buffered in a 1,000,000 capacity channel and flushed periodically or upon reaching the batch size limit.

---

## Subsystem Workflows

### 1. Administrative Control Plane

- **Ledger Auditing**: Campaign modifications update Postgres tables and generate audit records in `balance_ledger` within a single ACID transaction block (`pgx.BeginFunc`).
- **Transactional Outbox Pattern**:
  - Commits to Postgres write event payloads to `outbox_events` and invoke a trigger executing `pg_notify('outbox_channel', event_id)`.
  - A push-based `OutboxWorker` receives notifications via `LISTEN outbox_channel`.
  - Leases batches using `FOR UPDATE SKIP LOCKED` inside a short database transaction.
  - Commits the transaction, then executes Redis I/O (pipelining updates) outside Postgres transaction boundaries.
  - Updates the outbox event state to `'PROCESSED'` in a final batch write.
- **Closed-Loop Pacing Controller**:
  - A background `PacingControllerWorker` executes at designated intervals.
  - Compares actual spend against targeted profiles, adjusting pacing state (ASAP/EVEN) in PostgreSQL and emitting outbox invalidation signals to Redis.
  - Operates strictly on scaled `int64` micro-units (10^6) to prevent float rounding precision issues.

### 2. Ingestion & Settlement

- **Dynamic Geo Bid Floor Verification**:
  - Tracker replicas load publisher floor limits per country into a thread-safe `geoFloors sync.Map`.
  - Client IPs are mapped to ISO country codes via MaxMind databases.
  - If a floor is configured, a DFA scanner `parseBidMicro` traverses the raw payload linearly to extract the bid value without reflection or allocation. Bids below the floor are rejected with `ErrBidFloorNotMet`.
- **Atomic Edge Lua Evaluation**:
  - Tracker computes a static slot index: `crc32IEEE(id) & 1023`.
  - Executes a unified, atomic Lua script on the designated Redis shard to verify IP blacklists, deduplicate clicks (45s TTL), enforce frequency capping, and reserve micro-budget.
- **Exactly-Once Persistence**:
  - **PostgreSQL**: Workers enforce transactional idempotency via `ON CONFLICT DO NOTHING` against a `sync_idempotency` table.
  - **ClickHouse**: Writes are buffered in memory and flushed. Already persisted events (with `InsertedToCH` set to true) are skipped during retries to prevent analytics duplication.
  - **Janitor Loop**: Monitors stream groups and executes `XAutoClaim` to recover orphaned messages from the Pending Entries List (PEL). Messages exceeding `maxRetries` are archived to `ad:events:dlq` and deleted from the main queue.
- **PostgreSQL Partition Rotation**:
  - The partition manager runs daily to create future partitions and drop expired ones. It truncates `events_default` before creating new partitions to avoid constraint violations.

### 3. Log Broker (ELB) Implementation

- **Storage Engine**:
  - Leverages memory-mapped files (`syscall.Mmap`) for append-only log segments and companion index files.
  - Active segments automatically roll over to read-only segments when file sizes cross segment limits.
  - Sparse indexing records offset maps at configurable intervals. Searches on index offsets utilize binary search for log position lookup.
  - Startup recovery scans the tail of active segments, validates length headers, and truncates partial writes to recover from crashes.
- **Frame Format**:
  - Frames are serialized in big-endian layout containing a 4-byte CRC32 checksum appended at the end.
  - Supports topic registration (`CmdRegisterTopic`) and batch producing (`CmdProduceBatch`).
  - Batch messages prefix payloads with `BatchMsgHeader` containing `TopicID` and `PayloadLen`.
- **Log Encryption & Compression**:
  - Active log files are compressed with `zstd` and encrypted using `AES-GCM` with a 12-byte incrementing nonce.
  - The encryption key is derived via PBKDF2 with SHA-256 and a predefined salt from the `LOG_ENCRYPTION_KEY` environment variable.
  - Rotated segments use the `.log.zst.ready` suffix.
  - Decryption and decompression are executed via the `DecryptSegment` helper.
- **Lock-Free Concurrency**:
  - Employs an RCU-like snapshot pattern (`atomic.Pointer[segmentSnapshot]`). Updates to segment listings swap pointers atomically, eliminating read-write mutex locks on the read path.
  - Buffers for fetches are recycled using a `sync.Pool` to eliminate heap allocations.
  - Transient network data is cloned before storing topic keys to prevent memory corruption from recycled network buffers.
- **Replication Coordination**:
  - Redis acts as the coordinator storing leader leases and nodes metadata.
  - Active-passive replication loops on follower nodes pull from leaders via non-blocking consumers.
  - Health checks query memory state flags updated by background workers, bypassing synchronous disk writes.

---

## Observability & Alert Thresholds

- **CircuitBreakerOpen**: Alert fires if consumer group circuit breaker remains open for >5 minutes.
- **DatabaseWriteErrors**: Alert triggers on batch persistence/ledger failures.
- **DeadLetterQueueSpike**: Alert triggers if DLQ length exceeds 100 messages.
- **HighRequestLatency**: Alert triggers if p99 ingestion tracker latency climbs above 15ms.
- **Telegram Alert Proxy**: Alertmanager alerts are routed to a custom proxy daemon (`cmd/telegram.go`) that formats the JSON event into HTML and posts it to the Telegram Bot API.
