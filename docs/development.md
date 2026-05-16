# Development Guide

Tooling, testing, and maintenance workflow for the sharded ingestion pipeline.

## Requirements
- Go 1.25+
- Docker & Docker Compose
- `buf` (for Protobuf generation)
- `k6` (for performance benchmarking)

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
| **Grafana** | 3100 | Visualization (Host Mode) |

## Testing & Benchmarking

### Performance Tests
Located in `tests/load/`. Use `k6` to validate throughput and latency.
```bash
# Run load test
docker compose run --rm k6 run /scripts/rps_100k.js
```

### Integration Tests
Integration tests require the full infrastructure stack to be running.
- `tests/e2e_test.go`: Validates sharding and Protobuf ingestion.
- `tests/budget_test.go`: Validates Redis-to-Postgres budget synchronization across shards.

## Debugging
- **pprof**: Enabled on trackers (ports 8181-8184) and processor (8186).
- **Logs**: Structured JSON logs via `slog`. Use `docker compose logs -f <service>` for real-time monitoring.
- **Metrics**: Access Grafana at `http://localhost:3100` (anonymous admin access enabled).
