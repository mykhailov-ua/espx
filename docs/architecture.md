# Ad Event Processor Architecture Specification

Technical overview of the ad event ingestion pipeline and storage architecture.

## Ingestion Pipeline

### HTTP Ingress
*   **Protocol**: HTTP/1.1 or HTTP/3 (via Caddy).
*   **Format**: JSON or Protobuf (application/x-protobuf).
*   **IP Extraction**: Right-to-Left parsing of `X-Forwarded-For` to bypass private network spoofing.
*   **Filtering**: 
    *   IP-based rate limiting (Redis).
    *   Deduplication via LUA-based idempotency checks.
    *   **Fail-Open Policy**: Infrastructure errors (e.g. Redis outage) trigger a fail-open state where events are logged but accepted to prevent data loss.
*   **Validation**: Synchronous check against in-memory Campaign Registry.

### Budget Management
*   **Atomic Checks**: LUA script performs atomic budget verification and reservation in Redis.
*   **Cold Start Logic**: Automatic PostgreSQL fallback seeds Redis cache on cache-miss to prevent false budget exhaustion errors.
*   **Synchronization**: Periodic `SyncWorker` reconciles spent amounts from Redis to PostgreSQL using a non-blocking `SSCAN` and atomic `Read-Update-Decrement` pattern to eliminate data loss during worker crashes.

### Message Backbone (Redis Streams)
*   **Stream**: `ad:events:stream`.
*   **Retention**: `MAXLEN ~100000`.
*   **Consumer Groups**: Separate groups for PostgreSQL and ClickHouse sinks.

## Storage Strategy (T-Split)

Events are consumed by multiple `StreamConsumer` instances using separate Redis Consumer Groups.

### PostgreSQL Sink (`group_pg`)
*   **Data**: Event logs and real-time statistics.
*   **Partitioning**: Daily time-based partitioning on `created_date`.
*   **Aggregation**: Atomic "Insert + Update" via SQL CTEs with explicit `ORDER BY` to prevent database deadlocks.
*   **Worker Count**: Configurable (default 16).

### ClickHouse Sink (`group_ch`)
*   **Data**: Long-term analytical logs.
*   **Retention**: 180 days via TTL.
*   **Table Engine**: `ReplacingMergeTree`.
*   **Batch Size**: 50,000 events.

## Reliability Mechanisms

### Poison Pill Protection
Workers implement a `maxRetries` threshold (5). Batches exceeding this limit are forcibly acknowledged and logged as "Poison Pills" to prevent pipeline stalls caused by permanent data constraints.

### Pending Entries List (PEL) Recovery
Consumers recover assigned messages (ID "0") on startup and during graceful shutdown to ensure zero data loss.

### XAutoClaim Janitor
Background task reclaims messages from inactive consumers (>5 mins idle).

### Memory Hardening
`domain.Event` pool implements a safety cap on buffer capacity. Slices exceeding 4096 bytes are discarded during `Reset()` to prevent `sync.Pool` memory bloat under large-payload attacks.

## Shutdown Sequence

1.  HTTP Server: Graceful stop.
2.  Consumer Shutdown: Signal workers to stop reading.
3.  Drain Phase: Final buffer flush and PEL recovery using 15s timeout.
4.  Connection Cleanup: Final close of Redis, PG, and CH pools.
