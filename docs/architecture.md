# Ad Event Processor Architecture Specification

Technical overview of the ad event ingestion pipeline and storage architecture.

## System Design

The system is partitioned into two functional domains to isolate network ingestion from intensive database I/O:
1.  **Tracker (Ingress)**: Stateless HTTP server that validates and persists events to Redis Streams.
2.  **Processor (Egress)**: Stateful consumer workers that read from Redis Streams and write to PostgreSQL and ClickHouse.

## Ingestion Pipeline (Tracker)

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
*   **Consumer Groups**: Separate groups (`group_pg`, `group_ch`) for PostgreSQL and ClickHouse sinks to allow independent scaling and failure isolation.

## Processing Strategy (Processor)

Events are consumed by `StreamConsumer` instances using separate Redis Consumer Groups.

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

## Reliability and Fault Tolerance

### Circuit Breaker
Each `StreamConsumer` implements a lock-free state machine (Circuit Breaker) to protect downstream databases from cascading failures.
*   **Trigger**: Trips to `Open` state after `failThreshold` consecutive failures.
*   **Behavior**: When `Open`, the consumer pauses `XReadGroup` calls for a `openTimeout` duration.
*   **Recovery**: Transitions to `HalfOpen` for a single probe request. If successful, it returns to `Closed`.

### Dead Letter Queue (DLQ)
Messages that fail processing after `maxRetries` (5) are moved to the `ad:events:dlq` stream.
*   **Metadata**: DLQ entries include original message IDs, error messages, retry counts, and timestamps.
*   **Rationale**: Prevents "Poison Pills" from blocking the main pipeline while preserving erroneous events for manual inspection or replay.

### XAutoClaim Janitor
Background task reclaims messages from inactive consumers (>5 mins idle) to ensure processing continuity during worker crashes.

### Memory Hardening
`domain.Event` pool implements a safety cap on buffer capacity. Slices exceeding 4096 bytes are discarded during `Reset()` to prevent `sync.Pool` memory bloat under large-payload attacks.

## Observability

*   **Metrics**: Prometheus integration tracking ingestion rates, batch latencies, DLQ size, and Circuit Breaker states.
*   **Tracing**: Trace ID propagation through `context.Context` (internal).

## Shutdown Sequence

1.  HTTP Server: Graceful stop (Tracker).
2.  Consumer Shutdown: Signal workers to stop reading (Processor).
3.  Drain Phase: Final buffer flush and PEL recovery using 15s timeout.
4.  Connection Cleanup: Final close of Redis, PG, and CH pools.
