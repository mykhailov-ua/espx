# eSPX Architecture Specification

## Topology

The pipeline consists of five layers:

1. **Ingress (Nginx)**: Edge load balancer. Routes `/admin/*` to the Control Plane and `/track/*` to the Ingestion Plane.
2. **Control Plane**:
   - **Management Gateway (`:8188`)**: Exposes REST interfaces for campaign management, RBAC verification, and outbox event logging.
   - **Auth Service (`:51051`)**: gRPC microservice for Argon2id hashing and PASETO token generation.
3. **Ingestion Plane (Trackers)**:
   - Stateless Go instances (`:8181-8184`) running in host networking mode.
   - Event-driven I/O via `gnet/v2` with 2 event loops per instance. OS thread locking is disabled (`gnet.WithLockOSThread(false)`).
   - Task dispatch to workers (`PinnedWorkerPool`) using lock-free MPSC ring buffers.
   - Zero-allocation connection-local pool (`connContext`) bound to connection lifetime.
   - Zero-copy DFA HTTP/1.1 stream parser mapping headers directly from socket ring buffers.
4. **Caching State (Redis)**:
   - 6-node Redis cluster sharded via client-side `StaticSlotSharder` using O(1) constant-time lookup.
   - Executes atomic pipelined commands for rate limiting, budget allocation, pacing checks, and frequency capping.
   - Ingress load balancer parses JSON and Protobuf request payloads directly in Lua. It extracts the campaign_id and user_id to construct a composite_key (falling back to client_ip if empty) and hashes this key via ngx.crc32_long to determine the target Redis shard. This completely resolves IP-based hotspotting behind NAT gateways. Blacklist check results are cached locally in a shared dictionary (`blacklist_cache`) with a 300-second TTL.
5. **Settlement & Storage**:
   - **Processor Pool (`:8186`)**: Background consumer workers fetching stream batches from Redis Consumer Groups.
   - **PostgreSQL 16**: Relational storage for accounts, ledgers, audit logs, and daily partition tables.
   - **ClickHouse**: Columnar data warehouse for event telemetry. If batching is active (batch size > 1), events are buffered in a 1,000,000 capacity channel and flushed periodically or upon reaching the batch size limit.

---

## Subsystem Workflows

### 1. Administrative Control Plane

- **Ledger Auditing**: Campaign modifications update Postgres tables and generate audit records in `balance_ledger` within a single ACID transaction block (`pgx.BeginFunc`).
- **Transactional Outbox Pattern**:
  - Commits to Postgres write event payloads to `outbox_events`.
  - A polling-based `OutboxWorker` executes a highly efficient background loop.
  - Leases pending event batches using `SELECT * FROM outbox_events WHERE status = 'PENDING' ORDER BY created_at ASC LIMIT 1000 FOR UPDATE SKIP LOCKED` inside a short database transaction, setting status to `'PROCESSING'`. This avoids the RAM overhead and potential queue bloat of LISTEN/NOTIFY under heavy consumer lag.
  - Commits the transaction, then executes Redis I/O (pipelining updates) outside Postgres transaction boundaries to prevent connection pool starvation.
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
- **Atomic Edge Evaluation & Rate Limiting**:
  - Tracker computes a static slot index: `crc32Castagnoli(&id) & 1023`.
  - Executes atomic pipelined Redis operations on the designated shard to verify IP blacklists, deduplicate clicks (45s TTL), enforce frequency capping, and reserve micro-budget.
  - Custom Redis Lua scripts for IP rate limiting and login lockout are replaced with high-performance pipelines executing `INCR` and `PEXPIRE NX` / `EXPIRE NX` to reduce Redis CPU engine lock times and CPU usage.
  - The management service replicates IP blocks/unblocks to all Redis shards concurrently, ensuring global blacklist consistency across the entire cluster.
- **Exactly-Once Persistence**:
  - **PostgreSQL**: Workers enforce transactional idempotency via `ON CONFLICT DO NOTHING` against a `sync_idempotency` table.
  - **ClickHouse**: Volatile in-memory tracking has been deprecated. Achieves native exactly-once delivery by enabling `insert_deduplicate=1` settings in ClickHouse's `ReplicatedMergeTree`. Utilizes deterministic, stable block tokens computed via SHA-256 over event click IDs and timestamps (offset ranges).
  - **Janitor Loop**: Monitors stream groups and executes `XAutoClaim` to recover orphaned messages from the Pending Entries List (PEL). Messages exceeding `maxRetries` are archived to `ad:events:dlq` and deleted from the main queue.
- **Deterministic Lock Ordering & Batch Updates**:
  - Prevents PostgreSQL row-level deadlocks on the `campaign_stats` table by strictly ordering all batch updates by `campaign_id` and `event_date` (using explicit CTE ordering: `ORDER BY campaign_id, event_date` before executing `ON CONFLICT DO UPDATE`).
- **PostgreSQL Partition Rotation**:
  - The partition manager runs daily to create future partitions and drop expired ones. It truncates `events_default` before creating new partitions to avoid constraint violations.

### 3. Log Broker (ELB) Implementation

- **Storage Engine**:
  - Leverages memory-mapped files (`syscall.Mmap`) for append-only log segments and companion index files.
  - Active segments automatically roll over to read-only segments when file sizes cross segment limits.
  - Sparse indexing records offset maps at configurable intervals. Searches on index offsets utilize binary search for log position lookup.
  - Startup recovery scans the tail of active segments, validates length headers, and truncates partial writes to recover from crashes.
- **Zero-Copy & Hardware-Accelerated Writes**:
  - Writes to log and index mmap streams utilize direct `unsafe.Pointer` casting and hardware-optimized byte-swapping (`bits.ReverseBytes32`, `bits.ReverseBytes64`) to achieve absolute zero-copy writes and `0 B/op` allocations under benchmark.
  - On `amd64` platforms, hashing and slot allocation employ a custom hardware SSE4.2 Assembly routine (`CRC32Q`) for hardware-accelerated CRC32-C (Castagnoli) calculations with no memory allocations. Non-amd64 systems fall back to table-driven standard library CRC32.
- **Frame Format**:
  - Frames are serialized in big-endian layout containing a 4-byte CRC32 checksum appended at the end.
  - Supports topic registration (`CmdRegisterTopic`) and batch producing (`CmdProduceBatch`).
  - Batch messages prefix payloads with `BatchMsgHeader` containing `TopicID` and `PayloadLen`.
- **Log Encryption & Compression Decoupling**:
  - Removed synchronous `zstd` compression and `AES-GCM` encryption from the ingestion loop to avoid CPU saturation and O(N^2) write amplification.
  - Active log segments are flushed directly to disk as raw `.log` files, with background OS-level page flushing via `syscall.Fdatasync`.
  - A background `StartCompressorWorker` asynchronously scans the log directory, compresses rotated segments using `zstd` (with encoder recycling via a sync.Pool), and encrypts them using `AES-GCM` with a 12-byte incrementing nonce (derived via PBKDF2 with SHA-256 and a salt from `LOG_ENCRYPTION_KEY`). Output files use the `.log.zst.ready` suffix; original raw `.log` files are safely purged.
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
