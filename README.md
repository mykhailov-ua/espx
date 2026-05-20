# ad-event-processor

Ad event ingestion and processing pipeline.

## Core Features

- **Ingestion**: HTTP/Protobuf tracker utilizing pre-allocated domain UUID string registry and 64-bit integer micro-budget representations (1,000,000 / 10^6 scale) to achieve zero heap allocations on the hot path.
- **Validation**: Sharded Redis with atomic Lua filters (Budget, Pacing, Frequency).
- **Anti-Fraud**: 
  - DC/VPN/Proxy detection (MaxMind).
  - TTC (Time-to-Click) velocity checks.
  - Geo-targeting validation.
- **Persistence**: Decoupled transactional outbox using `LISTEN/NOTIFY` push events and short database transactions with `FOR UPDATE SKIP LOCKED` to decouple PostgreSQL from Redis database operations.
- **Serialization & DLQ**: Serialized binary Protobuf messages (`AdStreamEvent` and `AdDLQEvent`) using object pooling. DLQ CLI manager (`cmd/dlq-tool`) utilizing a length-prefixed binary format.
- **Management**: Background workers for Nginx IP blacklisting and DB partition rotation.

## Architecture

### Ingestion (Tracker)
- **Scaling**: Independent replicas behind Nginx load balancer.
- **Allocation Profile**: Zero heap allocations on the hot path via `sync.Pool` object recycling, pre-generated UUID domain cache, and integer budget arithmetic.
- **Network**: Host mode networking.

### State (Sharded Redis)
- **Sharding**: Consistent hashing by `CampaignID`.
- **Deduplication**: 45s TTL for ClickIDs.
- **Pacing**: Even and ASAP distribution modes.

### Persistence (Async Processor)
- **Consumer**: Redis Streams consumer groups processing binary Protobuf payloads (`AdStreamEvent`) with DLQ fallback (`AdDLQEvent`) and Circuit Breaker.
- **Outbox Worker**: Push-based daemon reacting to `LISTEN outbox_channel`. Acquires batch leases via `FOR UPDATE SKIP LOCKED` to minimize Postgres locks.
- **Storage**:
  - **PostgreSQL**: Daily partitions for event aggregates.
  - **ClickHouse**: 90-day TTL for raw event logs.

## Design Decisions

| Component | Decision | Rationale |
|-----------|----------|-----------|
| Serialization | Protobuf | Binary serialization format. |
| Networking | Host Mode | Direct access to host network stack. |
| Memory | sync.Pool | Buffer and object reuse. |
| Memory | GOMEMLIMIT | Hard memory limit for the Go runtime. |
| Budget Scaling | 64-bit integer representation | Eliminates heap allocations from decimal parsing. |
| ID Processing | Pre-generated UUID string cache | Prevents heap allocations from string formatting. |
| Outbox Pattern | LISTEN/NOTIFY + SKIP LOCKED | Decouples database transaction scope from Redis write operations. |
| DLQ Storage | Length-prefixed binary Protobuf | Reduces disk space utilization and eliminates JSON parsing overhead by utilizing a 4-byte Big-Endian size prefix and raw bytes. |
| Persistence | Redis Streams | Decoupling ingestion from database writes. |

## Deployment

### Requirements
- Docker Engine / Docker Compose.
- 16GB RAM.

### Resource Limits
- ClickHouse: 4GB RAM.
- Redis Shards: 768MB each.
- Trackers: GOGC=50.

## Observability
- **Grafana**: Pre-configured dashboards for ingestion and database performance.
- **Prometheus**: Metrics from all internal components.
- **Health Checks**: Connectivity verification for all dependencies.

## Scaling
- Horizontal scaling of Tracker/Processor replicas.
- Redis sharding for state distribution.
- ClickHouse clustering for analytical volume.
