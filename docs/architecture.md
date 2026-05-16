# ad-event-processor Architecture Specification

Overview of the distributed real-time ad event ingestion pipeline, control plane, and dual storage persistence architecture.

## System Topology

The architecture is structured into five distinct operational layers communicating over standardized protocols:

1. **Ingress Layer (Nginx)**
   - Primary HTTP/3 reverse proxy terminating incoming client traffic.
   - Routes administrative REST API calls to the Control Plane (`/admin/*`) and high-frequency ad impression/click events to the Ingestion Plane (`/track/*`).

2. **Control Plane (Management & Authentication)**
   - **Management Gateway (`:8188`)**: Serves external REST endpoints, managing RBAC, DTO serialization, and financial ledger idempotency.
   - **Auth Service (`:51051`)**: Internal gRPC microservice handling `Argon2id` password hashing and issuing cryptographic PASETO tokens.

3. **Ingestion Plane (Tracker Replicas)**
   - Stateless Go instances (`:8181-8184`) running in `network_mode: host` to maximize kernel packet processing throughput.
   - Employs `sync.Pool` object recycling to maintain zero heap allocations on the hot path.

4. **Edge Caching Layer (Redis Shard Cluster)**
   - 6-node Redis cluster sharded via consistent JumpHash indexing.
   - Executes atomic Lua scripts for budget verification, deduplication, and IP blacklists, while buffering valid events in Redis Streams (`ad:events:stream`).

5. **Asynchronous Settlement & Storage**
   - **Processor Pool (`:8186`)**: Background consumer workers fetching stream batches and executing multi-row database updates.
   - **PostgreSQL 16**: Primary relational storage for ACID transactions, ledger balances (`balance_ledger`), and daily table partitions.
   - **ClickHouse**: Columnar analytical data warehouse storing raw ad telemetry and anti-fraud anomaly logs.

## Core Subsystems & Request Lifecycles

### 1. Management Control Plane Lifecycle
1. **Ingress & Authentication**: Requests to `/admin/*` arrive at the Management Gateway (`:8188`). The gateway authenticates incoming calls by intercepting HTTP cookies (`accessToken` and `refreshToken`). Cryptographic PASETO verification occurs entirely in memory.
2. **Session Revocation & RBAC**: The gateway checks token revocation status against Redis (`revoked:token:{id}`) with a 100ms circuit breaker. The user's role (`SA`, `M`, `C`, `G`) is evaluated against endpoint permissions. For Customer (`C`) requests, the gateway enforces a hard data isolation filter by extracting `CustomerID` from the token payload.
3. **Database Execution**: All state-modifying operations (e.g., `TopUpBalance`, `CreateCampaign`, `BlockIP`, `UpdateSettings`) execute within a strict PostgreSQL ACID transaction (`pgx.BeginFunc`). Write operations generate immutable ledger entries in `balance_ledger` and log administrative actions in `admin_audit_log`.
4. **Cache Replication**: Successful database modifications trigger cache updates in the sharded Redis cluster. To ensure eventual consistency across network partitions, a background synchronization worker (`RunSystemStateSyncer`) reconciles PostgreSQL state with Redis every 60 seconds.

### 2. Ad Event Ingestion & Processing Lifecycle
1. **Ingress**: Telemetry events (impressions and clicks) reach Tracker replicas (`:8181-8184`) via Nginx over HTTP/3 in Protobuf or JSON format. Replicas operate in `network_mode: host` to bypass Docker bridge NAT translation and utilize `sync.Pool` to minimize heap memory allocations.
2. **Atomic Edge Lua Evaluation**: The tracker computes a consistent JumpHash on `CampaignID` to locate the assigned Redis shard. It executes a unified, atomic Lua script that verifies IP blacklists, deduplicates clicks, enforces user frequency capping, and reserves the micro-budget. If validation fails, the event is dropped or flagged for fraud analysis.
3. **Stream Queuing**: Validated events are appended to the Redis Stream `ad:events:stream`.
4. **Asynchronous Settlement**: Processor pool workers (`:8186`) consume event batches from Redis Streams via Consumer Groups. Deduplicated events trigger multi-row batch updates in PostgreSQL to deduct campaign budgets and customer balances. Simultaneously, raw event logs and anti-fraud telemetry are written to ClickHouse via memory-mapped buffers for columnar analytics.

## Storage Specifications & Scaling Strategy

### Storage Engines
* **PostgreSQL 16**: Relational master database storing customer accounts, financial ledgers (`balance_ledger`), RBAC permissions, and campaign metadata. Database access is managed via type-safe `sqlc` queries with daily table partitioning.
* **ClickHouse**: Columnar analytical store designed for click/impression telemetry, aggregation queries, and anti-fraud anomaly detection.
* **Redis Cluster**: Sharded 6-node in-memory storage layer providing edge validation for active budgets, token revocation flags, IP blacklists, and asynchronous streaming queues.

### Scalability Strategy
* **Horizontal Scaling**: All ingestion trackers, batch processors, and management gateways are stateless and scale horizontally across container nodes.
* **Sharding Architecture**: In-memory state is sharded across multiple Redis instances using consistent JumpHash indexing, ensuring uniform load distribution without cross-node lock contention.
