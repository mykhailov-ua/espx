# eSPX (Event Stream Pacing)

Event ingestion and pacing pipeline.

## Core Features

- **Ingestion**: Event-driven network handler based on `github.com/panjf2000/gnet/v2` with `SO_REUSEPORT` and `TCP_NODELAY` socket configurations.
- **Validation**: Sharded Redis cluster utilizing client-side static hash slot mapping for budget, pacing, and frequency checks.
- **Anti-Fraud**: MaxMind DC/VPN/Proxy checks, Time-To-Click (TTC) velocity checks, and geo-targeting validation.
- **Persistence**: Transactional outbox pattern using a high-throughput Postgres polling loop (`SKIP LOCKED`) and asynchronous multi-row batch writers.
- **Serialization**: Schema-optimized binary Protobuf formats utilizing zero-copy unmarshaling via `vtproto`.
- **Infrastructure**: Automated PostgreSQL partition rotation, concurrent multi-shard Redis blacklist sync workers, and Nginx dynamic Lua-based load balancing.

---

## Ingestion Architecture

### Ingress (Tracker)
- **Networking**: Stateless replicas running in host network mode using `gnet/v2` with 2 event loops per instance and OS thread locking disabled (`gnet.WithLockOSThread(false)`).
- **Worker Pool**: Task dispatch to CPU-pinned workers using lock-free MPSC ring buffers with cache-line padding.
- **Memory Footprint**: Lock-free, zero-allocation connection-local pool (`connContext`) bound to connection lifetime.
- **Data Parsing**: Zero-copy DFA HTTP/1.1 request stream scanner mapping headers directly from socket ring buffers.

### Edge Caching & Routing
- **Sharding**: Client-side `StaticSlotSharder` executing O(1) constant-time lookups over 1024 virtual slots.
- **Filters**: Atomic pipelined Redis evaluations checking budget allocations, click deduplication, and frequency caps.
- **Blacklist Cache**: Nginx Lua local shared dictionary (`blacklist_cache`) with 300-second TTL. It extracts campaign_id and user_id from JSON/Protobuf request bodies to hash via `ngx.crc32_long` to determine the target Redis shard, completely eliminating NAT-based shard hotspotting.

### Settlement
- **Processor**: Consumers pulling batch streams from Redis Consumer Groups with integrated Circuit Breaker.
- **Postgres 16**: ACID daily partitions with write idempotency tracking. Enforces strict deterministic sorting (`ORDER BY campaign_id, event_date`) in batch queries to completely eliminate row-level deadlocks under concurrency.
- **ClickHouse**: Columnar batch writes buffered via an in-memory 1,000,000 capacity channel to limit LSM part fragmentation. Guarantees exactly-once persistence using `ReplicatedMergeTree` with `insert_deduplicate=1` settings and stable SHA-256 event click ID block tokens.

---

## Design Decisions

| Subsystem | Selected Pattern | Justification |
| :--- | :--- | :--- |
| **Serialization** | Protobuf (`bytes` fields) | Bypasses reflective marshalling; permits zero-allocation slicing directly from stream buffers. |
| **Networking** | `gnet` + Worker Pool | Dispatches connection events to pinned OS threads via lock-free ring buffers. |
| **Sharding** | Static Slot Mapping | Bypasses JumpHash overhead. Achieves constant O(1) lookup via bitwise `key & 1023` masking. |
| **Memory** | Connection-Local Context | Eliminates global `sync.Pool` lock contention, interface boxing, and type assertion overhead. |
| **Budgeting** | Integer Scaling | Micro-unit integer representation (10^6) eliminating decimal/float parsing allocations. |
| **Outbox I/O** | `SKIP LOCKED` Polling Loop | Decouples PG transaction scope from Redis write operations, avoiding connection pool starvation and Postgres notification queue RAM bloat. |
| **Log Broker** | Asynchronous Compressor | Decouples compression and encryption from the hot path. Writes raw files directly with `syscall.Fdatasync` and processes them asynchronously via background worker. |
| **Zero-Copy Writes** | `unsafe.Pointer` + mmap | Employs memory-mapped files (`syscall.Mmap`) with unsafe pointers and CPU hardware assembly `CRC32Q` (SSE4.2) to achieve actual `0 B/op` writes. |
| **Rate Limiting** | Pipelined Atomic Commands | Replaces custom Lua scripts with pipelined atomic commands (`INCR` + `EXPIRE NX`/`PEXPIRE NX`) to reduce Redis engine lock time. |

---

## Observability

- **Metrics**: End-to-end telemetry scraped by Prometheus.
- **Visuals**: Grafana dashboards monitoring throughput, memory, and database latencies.
- **Alerting**: Alertmanager routes anomalies to Telegram webhook gateway.
