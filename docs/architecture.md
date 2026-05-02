# Ad Event Processor Architecture Specification

Technical overview of the ad event ingestion pipeline and storage architecture.

## Ingestion Pipeline

### HTTP Ingress
*   Protocol: HTTP/1.1 or HTTP/3 (via Caddy).
*   Format: JSON or Protobuf (application/x-protobuf).
*   Filtering: IP-based rate limiting and `click_id` deduplication via Redis.
*   Validation: Synchronous check against in-memory Campaign Registry.

### Message Backbone (Redis Streams)
*   Stream: `ad:events:stream` (configurable).
*   Retention: `MAXLEN ~100000` (approximate).
*   Consumer Groups: Independent groups for parallel ingestion into multiple sinks.

## Storage Strategy (T-Split)

Events are consumed by multiple `StreamConsumer` instances using separate Redis Consumer Groups.

### PostgreSQL Sink (`group_pg`)
*   Data: Raw event logs and campaign statistics.
*   Partitioning: Time-based partitioning on `created_at` (daily).
*   Aggregation: Atomic "Insert + Update" via SQL CTEs.
*   Worker Count: 16 (matched to DB connection pool).

### ClickHouse Sink (`group_ch`)
*   Data: Long-term analytical logs.
*   Retention: 180 days via TTL.
*   Table Engine: `ReplacingMergeTree` (partitioned by month).
*   Batch Size: 50,000 events.
*   Worker Count: 1.

## Reliability Mechanisms

### Pending Entries List (PEL) Recovery
Consumers check assigned PEL (ID "0") on startup to process unacknowledged messages from previous runs.

### XAutoClaim Janitor
Background task periodically reclaims messages from consumers idle for >5 minutes.

### Acknowledgment (ACK) Policy
`XAck` is invoked only after confirmed successful persistence in `EventStore`. Failed storage operations trigger backoff retry and leave message in the PEL.

## Shutdown Sequence

1.  HTTP Server: Stop accepting new requests.
2.  Context Cancellation: Signal `StreamConsumer` workers to stop reading.
3.  Drain Phase: Final flush of local buffers and exhaustion of PEL/Stream tail using dedicated context (15s timeout).
4.  Connection Cleanup: Close Redis, PostgreSQL, and ClickHouse pools.
