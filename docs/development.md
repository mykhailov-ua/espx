# Development Guide

Tooling, testing, and maintenance workflow for the sharded ingestion pipeline.

## Requirements
- Go 1.25+
- Docker & Docker Compose
- `buf` (for Protobuf generation)

## Makefile Targets

| Target | Action |
| :--- | :--- |
| `make fmt` | Format code via `go fmt`. |
| `make proto` | Generate Go code from Protobuf definitions using `buf`. |
| `make test` | Run all tests (unit + integration). |
| `make build` | Build production Docker image. |

## Local CI Emulation (Pre-push)

To prevent CI/CD failures due to resource constraints (e.g., deadlocks under low CPU/RAM), the project uses `act` and `Lefthook` to simulate the GitHub Actions environment locally before code is pushed.

### Hardware Simulation
- **CPU**: 2 Cores
- **RAM**: 7 GB
- **Environment**: `catthehacker/ubuntu:act-latest` (Docker-in-Docker enabled)

### Commands
| Action | Command |
| :--- | :--- |
| **Manual CI Run** | `act -j all-in-one` |
| **Install Hooks** | `lefthook install` |

### Configuration
- `.actrc`: Configures resource limits and Docker socket mapping.
- `lefthook.yml`: Configures the `pre-push` gatekeeper.

## Local Infrastructure
The system uses a sharded infrastructure.

```bash
# Start 4 Trackers, 6 Redis Shards, PG, CH, and Monitoring
docker compose up -d
```

### Port Mapping
| Service | Port(s) | Description |
| :--- | :--- | :--- |
| **Nginx** | 8180 | Edge Load Balancer |
| **Tracker (0-3)** | 8181-8184 | Sharded Ingestion Replicas (Host Mode) |
| **Processor** | 8186 | Async Worker (Metrics/Health) |
| **Management** | 8188 | Control Plane Gateway |
| **Auth Server** | 51051 | Internal gRPC Auth Server |
| **Redis Shards** | 6479-6484 | Sharded Cache Cluster |
| **PostgreSQL** | 5440 | Transactional Database |
| **ClickHouse** | 9100, 8223 | Analytical Database |
| **Prometheus** | 9190 | Metrics Storage (Host Mode) |
| **Alertmanager** | 9093 | Alert Routing Engine |
| **Telegram Alert Proxy** | 8222 | Telegram Webhook Gateway |
| **Grafana** | 3100 | Visualization (Host Mode) |

## Staging Setup & GeoIP Database Preparation

To run country and VPN detection, MaxMind GeoIP databases must be downloaded manually and placed in the project directory before starting the services:

1. Create the GeoIP storage directory:
   ```bash
   mkdir -p deploy/geoip
   ```
2. Place the following binary databases into `deploy/geoip/`:
   * `GeoLite2-Country.mmdb` (Country targeting)
   * `GeoLite2-Anonymous.mmdb` (Proxy/VPN/Hosting identification)

The docker-compose volumes mount `deploy/geoip` onto the stateless tracker replicas. If these files are missing, the GeoIP module will default to allowing all requests to prevent blocking traffic.

## Testing & Benchmarking

### Integration Tests
Integration tests require the full infrastructure stack to be running.
- `tests/e2e_test.go`: Validates sharding and Protobuf ingestion.
- `tests/budget_test.go`: Validates Redis-to-Postgres budget synchronization across shards.

## Debugging
- **pprof**: Enabled on trackers (ports 8181-8184) and processor (8186).
- **Logs**: Structured JSON logs via `slog`. Use `docker compose logs -f <service>` for real-time monitoring.
- **Metrics**: Access Grafana at `http://localhost:3100` (anonymous admin access enabled).

## CLI Management Tools

### DLQ Management Tool (`cmd/dlq-tool`)
The DLQ tool handles archiving, restoring, requeueing, and inspecting events from the Dead Letter Queue stream.

#### Commands and Operations:
* **Archive DLQ events to disk**:
  ```bash
  go run cmd/dlq-tool/main.go -action=archive -stream=ad:events:dlq -dest=dlq_archive.bin -batch=1000
  ```
  Extracts unprocessable events from the specified Redis stream, packages them into serialized Protobuf `AdDLQEvent` payloads, writes them to the destination file using a length-prefixed format (4-byte Big-Endian size prefix + message bytes), and removes successfully written messages from the Redis stream.

* **Restore archived DLQ events from disk**:
  ```bash
  go run cmd/dlq-tool/main.go -action=restore -dest=dlq_archive.bin -stream=ad:events -batch=1000
  ```
  Reads the length-prefixed binary `AdDLQEvent` payloads from the file, extracts the embedded original `AdStreamEvent` payload, and pushes them back into the active processing stream (e.g. `ad:events`) in Redis via batch pipelines.

* **Requeue DLQ events directly in Redis**:
  ```bash
  go run cmd/dlq-tool/main.go -action=requeue -stream=ad:events:dlq -dest=ad:events -batch=1000
  ```
  Pushes unprocessable events from the DLQ stream directly back into the target active event stream in Redis without disk I/O.

* **Inspect live stream entries**:
  ```bash
  go run cmd/dlq-tool/main.go -action=inspect -stream=ad:events:dlq
  ```
  Prints human-readable representations of live stream messages (decoding Protobuf structures and falling back to legacy flat-maps).

