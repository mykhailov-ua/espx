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
   - Stateless Go instances (`:8181-8184`) running in `network_mode: host` to bypass bridge network layers and optimize packet processing throughput.
   - Employs `sync.Pool` object recycling to maintain zero heap allocations on the ingestion path.
   - Eliminates allocations on the hot path by utilizing pre-generated domain UUID strings from a registry cache and representing budgets as 64-bit integers scaled by 1,000,000 (10^6) to avoid decimal parsing overhead.

4. **Edge Caching Layer (Redis Shard Cluster)**
   - 6-node Redis cluster sharded via consistent JumpHash indexing.
   - Executes atomic Lua scripts for budget verification, deduplication, and IP blacklists, while buffering valid events as serialized binary Protobuf `AdStreamEvent` payloads in Redis Streams (`ad:events:stream`).

5. **Asynchronous Settlement & Storage**
   - **Processor Pool (`:8186`)**: Background consumer workers fetching stream batches and executing multi-row database updates.
   - **PostgreSQL 16**: Primary relational storage for ACID transactions, ledger balances (`balance_ledger`), and daily table partitions.
   - **ClickHouse**: Columnar analytical data warehouse storing raw ad telemetry and anti-fraud anomaly logs.

6. **Observability & Alerting**
   - **Prometheus (`:9190`)**: Scrapes real-time metrics from trackers, gateways, and processors. Evaluates alerting rules (`prometheus.rules.yml`).
   - **Alertmanager (`:9093`)**: Group and route fired rules to targets.
   - **Telegram Alert Proxy (`:8222`)**: Webhook endpoint transforming Prometheus JSON payloads into HTML and routing them to the Telegram Bot API.

## Core Subsystems & Request Lifecycles

### 1. Management Control Plane Lifecycle
1. **Ingress & Authentication**: Requests to `/admin/*` arrive at the Management Gateway (`:8188`). The gateway authenticates incoming calls by intercepting HTTP cookies (`accessToken` and `refreshToken`). Cryptographic PASETO verification occurs entirely in memory.
2. **Session Revocation & RBAC**: The gateway checks token revocation status against Redis (`revoked:token:{id}`) with a 100ms circuit breaker. The user's role (`SA`, `M`, `C`, `G`) is evaluated against endpoint permissions. For Customer (`C`) requests, the gateway enforces a hard data isolation filter by extracting `CustomerID` from the token payload.
3. **Database Execution**: All state-modifying operations (e.g., `TopUpBalance`, `CreateCampaign`, `BlockIP`, `UpdateSettings`) execute within a strict PostgreSQL ACID transaction (`pgx.BeginFunc`). Write operations generate immutable ledger entries in `balance_ledger` and log administrative actions in `admin_audit_log`.
4. **Transactional Outbox Replication**: Successful database modifications write an event payload to the `outbox_events` table within the same database transaction. Upon commit, a PostgreSQL trigger `outbox_event_trigger` executes `pg_notify('outbox_channel', event_id)`. A background `OutboxWorker` processes these events:
    - **Trigger/LISTEN Path**: The worker maintains a dedicated database connection running `LISTEN outbox_channel` and waits on `WaitForNotification`.
    - **Decoupled Transaction Pattern**: Upon a signal, the worker leases a batch of pending events to a `'PROCESSING'` state by executing a transaction with restricted scope (using `FOR UPDATE SKIP LOCKED`) and commits the transaction immediately. All Redis I/O (pipelined writes and Pub/Sub notifications) is then executed outside PostgreSQL database transaction boundaries. The worker then runs a final batch update to set status to `'PROCESSED'` or revert failed events to `'PENDING'`.
    - **Janitor Loop**: A periodic self-healing loop runs at 5 times the default interval to reset events stuck in `'PROCESSING'` for over 5 minutes and perform safety drain checks.


### 2. Ad Event Ingestion & Processing Lifecycle
1. **Ingress**: Telemetry events (impressions and clicks) reach Tracker replicas (`:8181-8184`) via Nginx over HTTP/3 in Protobuf or JSON format. Replicas operate in `network_mode: host` to bypass Docker bridge NAT translation. Ingestion uses pre-allocated domain UUID strings from a registry cache and represents campaign budgets as 64-bit integers scaled by 1,000,000 (10^6) to prevent heap allocations. Replicas utilize `sync.Pool` for payload buffer and event structure reuse.
2. **Atomic Edge Lua Evaluation**: The tracker computes a consistent JumpHash on `CampaignID` to locate the assigned Redis shard. It executes a unified, atomic Lua script that verifies IP blacklists, deduplicates clicks, enforces user frequency capping, and reserves the micro-budget. If validation fails, the event is dropped or flagged for fraud analysis.
3. **Stream Queuing**: Validated events are serialized as binary Protobuf `AdStreamEvent` payloads and appended to the Redis Stream `ad:events:stream`.
4. **Asynchronous Settlement & Exactly-Once Persistence**: Processor pool workers (`:8186`) consume event batches from Redis Streams via Consumer Groups. They settle these events using the following durability and isolation guarantees:
    - **Worker-Granular Circuit Breaker**: Thread-safe status transitions and worker-specific failure tracking maps isolate downstream write pressure. Bounding failure tracking to individual worker/shard scopes prevents a failing database or ClickHouse shard on one host from blocking independent, healthy ingestion pipelines.
    - **PostgreSQL Exactly-Once Settlement**: Asynchronous budget settlement updates are transactionally synchronized. To ensure exactly-once persistence during retry loops of previously claimed messages, workers execute writes within active PostgreSQL transactions using an `ON CONFLICT DO NOTHING` clause on a `sync_idempotency` ledger table.
    - **ClickHouse Partial Failure Deduplication**: Multi-table columnar batch insertions (routing to impressions, clicks, conversions, and fraud logs) track persistence status using an in-memory `InsertedToCH` flag per event. During batch retry attempts, the pipeline skips tables where the event has already been successfully written, preventing duplicate analytical log data.
    - **Self-Healing Janitor & DLQ Monitoring**: Background janitor routines monitor stream groups and execute `XAutoClaim` to recover orphaned messages stalled in the Pending Entries List (PEL). Message retries are capped at `maxRetries`; events exceeding this limit are atomically encapsulated into a binary Protobuf `AdDLQEvent` envelope, routed to the `ad:events:dlq` stream, and deleted from the main ingestion queue.

## Storage Specifications & Scaling Strategy

### Storage Engines
* **PostgreSQL 16**: Relational master database storing customer accounts, financial ledgers (`balance_ledger`), RBAC permissions, and campaign metadata. Database access is managed via type-safe `sqlc` queries with daily table partitioning.
* **ClickHouse**: Columnar analytical store designed for click/impression telemetry, aggregation queries, and anti-fraud anomaly detection.
* **Redis Cluster**: Sharded 6-node in-memory storage layer providing edge validation for active budgets, token revocation flags, IP blacklists, and asynchronous streaming queues.

### Scalability Strategy
* **Horizontal Scaling**: All ingestion trackers, batch processors, and management gateways are stateless and scale horizontally across container nodes.
* **Sharding Architecture**: In-memory state is sharded across multiple Redis instances using consistent JumpHash indexing, ensuring uniform load distribution without cross-node lock contention.

## Anti-Fraud & Geo-Targeting Execution
* **Geo-IP Verification**: Tracker replicas load MaxMind `GeoLite2-Country.mmdb` into memory. Incoming click/impression IPs are mapped to ISO country codes. If a campaign configures geo-targeting, non-matching countries trigger direct `ErrGeoBlocked` failures.
* **Datacenter/VPN Identification**: IPs are verified against MaxMind `GeoLite2-Anonymous.mmdb`. Telemetry originating from datacenters, public proxies, VPNs, or Tor exit nodes is tagged as fraud (`datacenter_ip`) and silently dropped from the main flow into the clickfraud analytics stream.
* **Time-To-Click (TTC) Velocity Capping**: Impressions write an expiration key `imp_ts:{UserID}:{CampaignID}` in Redis. Clicking within a time delta shorter than `ttcMin` (e.g. 500ms) flags the request as bot-generated (`low_ttc`).

## Observability & SLA Thresholds
* **Prometheus Alerting Rules**: Evaluated against the following metric thresholds:
  * `CircuitBreakerOpen`: Fired if downstream database pressure forces the Redis-to-database consumer group circuit breaker open for >5 minutes.
  * `DatabaseWriteErrors`: Triggers on failed batch persistence/ledger updates.
  * `DeadLetterQueueSpike`: Triggers if the unprocessable event dead-letter queue (DLQ) length exceeds 100 messages.
  * `HighRequestLatency`: Alerts if p99 ingestion tracker latency climbs above 15ms.
* **Telegram Routing Proxy**: When alerts transition to `firing`, Alertmanager posts JSON payloads to `/webhook`. The custom `alertmanager-telegram` daemon formats the event details into HTML and forwards them via the Telegram Bot API.
