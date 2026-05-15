# ad-event-processor

Ad event ingestion and processing pipeline.

## Core Features

- **Ingestion**: HTTP/Protobuf tracker with object pooling.
- **Validation**: Sharded Redis with atomic Lua filters (Budget, Pacing, Frequency).
- **Anti-Fraud**: 
  - DC/VPN/Proxy detection (MaxMind).
  - TTC (Time-to-Click) velocity checks.
  - Geo-targeting validation.
- **Persistence**: Async processing into PostgreSQL (Transactional) and ClickHouse (Analytical).
- **Management**: Background workers for Nginx IP blacklisting and DB partition rotation.

## Architecture

### Ingestion (Tracker)
- **Scaling**: Independent replicas behind Nginx load balancer.
- **State**: Stateless; offloaded to sharded Redis layer.
- **Network**: Host mode networking.

### State (Sharded Redis)
- **Sharding**: Consistent hashing by `CampaignID`.
- **Deduplication**: 45s TTL for ClickIDs.
- **Pacing**: Even and ASAP distribution modes.

### Persistence (Async Processor)
- **Consumer**: Redis Streams consumer groups with DLQ and Circuit Breaker.
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
