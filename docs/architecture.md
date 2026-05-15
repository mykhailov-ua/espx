# ad-event-processor Architecture Specification

Overview of the ad event ingestion pipeline and storage architecture.

## System Design

The system uses a distributed architecture to separate ingestion from persistence.
1.  **Tracker Pool (Ingress)**: Go replicas in `network_mode: host`.
2.  **Redis Shard Cluster (State)**: Redis instances for validation and transient state.
3.  **Processor Pool (Egress)**: Consumer workers for database persistence.
4.  **Management Service**: Control plane for budget and campaign metadata.

## Ingestion Pipeline (Tracker)

### HTTP Ingress
*   **Networking**: Host Network Mode.
*   **Protocol**: HTTP/1.1.
*   **Format**: Protobuf (`application/x-protobuf`) or JSON.
*   **Object Pooling**: `sync.Pool` for request bodies and response objects.

### Validation Engine
*   **Sharding**: Consistent hashing by `CampaignID` to Redis shards.
*   **Unified Lua Filter**: Atomic execution of:
    1.  Rate limiting (IP-based).
    2.  Deduplication (ClickID).
    3.  Budget Reservation (Campaign/Customer level).
    4.  Frequency Capping (User-based).
    5.  Pacing (Even/ASAP modes).
*   **Anti-Fraud**:
    1.  Geo-Targeting: Validation against allowed country list.
    2.  Anonymity Detection: Filtering DC/VPN/Proxy IPs via MaxMind.
    3.  TTC (Time-To-Click): Minimum interval between impression and click.
    4.  Silent Drop: Fraudulent events are redirected to a separate stream.

## Persistence Strategy (Processor)

### Stream Consumption
*   **Consumer Groups**: Shard-aware consumption from Redis Streams.
*   **DLQ**: Retries with exponential backoff; move to `ad:events:dlq` after exhaustion.
*   **Circuit Breaker**: Consumption pause on database write failures.

### Storage
*   **PostgreSQL**:
    *   Data: Transactional budget state and campaign metadata.
    *   Partitioning: Daily partitions on `events` table via background manager.
*   **ClickHouse**:
    *   Data: Analytical event logs and fraud telemetry.
    *   Sinks: Batch writes with memory-mapped buffers.

## Management & Operations

### IP Blacklisting
*   **Nginx Worker**: Background task exports blacklisted IPs from Redis to Nginx `deny` files.
*   **Blacklist Types**: Manual (admin) and Auto (fraud detection system).

### Health & Monitoring
*   **Prometheus**: Metrics for ingestion, drop rates, and database latencies.
*   **Health Checks**: Dependency verification (Postgres, ClickHouse, Redis shards).

## Deployment Topology

### Capacity per Node
- **CPU**: 12 Cores.
- **RAM**: 16GB.
- **Network**: Host networking.

### Scaling
- **Horizontal**: Deployment of additional Tracker/Processor replicas.
- **Sharding**: Increasing Redis instances with consistent hashing update.
