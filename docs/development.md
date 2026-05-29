# Development Guide

Tooling, testing, and maintenance workflow for the sharded ingestion pipeline.

## Requirements
- Go 1.25+
- Docker & Docker Compose
- `buf` (for Protobuf generation)

## Makefile & Taskfile Targets

For modern workflow automation, use the `Taskfile.yml` configuration (executable via `task` globally or `go run github.com/go-task/task/v3/cmd/task@latest`).

### Taskfile Tasks
| Task | Action |
| :--- | :--- |
| `task gen` | Run all codegen: `sqlc generate`, `templ generate`, and `buf generate`. |
| `task docker-up` | Start infrastructure databases (`db`, 6 Redis shards, `clickhouse`) in detached mode. |
| `task docker-down` | Stop and teardown all infrastructure containers. |
| `task check-deps` | Invoke the local database healthcheck shell script. |
| `task test-full` | Run the complete Go test suite with the race detector enabled (`go test -v -race ./...`). |

### Legacy Makefile Targets
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
| **Redis Shards** | 6479-6484 | Client-Side Sharded Cache Pool |
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

## Performance Gate CI/CD Pipeline

To ensure the primary `/ads` ingestion hot path maintains zero-allocation behavior and prevents latency regression, a strict Performance Gate is integrated into the CI/CD pipeline.

### Architectural Setup
* **Hardware Profile**: The benchmark checks must execute exclusively on a **Self-Hosted Dedicated Runner** (e.g., Hetzner CCX or bare-metal instances). Shared hosting environments (such as default GitHub-hosted runners) are prohibited due to virtual machine hypervisor steal time and CPU noise.
* **CPU Tuning**: The pipeline forces the host CPU governor into `performance` mode prior to execution to lock CPU frequency and eliminate scaling latency artifacts:
  ```bash
  echo "performance" | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
  ```

### Analysis & Gate Criteria
The execution utilizes `go test -bench=BenchmarkAdsPacketHandler -benchmem -count=10` on both baseline (`main` branch) and target PR code. The outputs are compared via `benchstat`:
1. **Memory Leak Control**: Any output showing `allocs/op > 0` fails the pipeline.
2. **Memory Bloat Control**: Any output showing `B/op > 0` fails the pipeline.
3. **CPU Regression Control**: Any time/op metrics demonstrating a latency regression exceeding `12.0%` with statistical significance (p < 0.05) fail the pipeline.

Upon a violation, the gate script `scripts/perf_gate.go` exits with code `1`, blocking the pull request merge.

### Local Execution & Testing
You can emulate the performance gate analysis locally.

#### Automated Local Task (Recommended)
You can run the complete performance gate simulation (staging base/PR branches in worktrees, profiling, and executing benchstat/perf_gate analysis) using Taskfile:
```bash
task perf-gate
```

#### Manual Native Bare-Metal Comparison
This method isolates the baseline in a separate git worktree to avoid dirtying your current branch or stash state:
```bash
# 1. Profile your current changes (PR state)
go test -run=^$ -bench=BenchmarkAdsPacketHandler -benchmem -count=10 ./internal/ads > pr_bench.txt

# 2. Check out the baseline (main branch) in an isolated worktree directory
git worktree prune || true
rm -rf ../baseline || true
git worktree add ../baseline main

# 3. Profile the baseline state
cd ../baseline
go test -run=^$ -bench=BenchmarkAdsPacketHandler -benchmem -count=10 ./internal/ads > ../ad-event-processor/baseline_bench.txt

# 4. Return to your working directory and execute comparison
cd ../ad-event-processor
go run scripts/perf_gate.go baseline_bench.txt pr_bench.txt
```

#### Local GitHub Actions Runner Emulation
To run the automated workflow using `act`:
```bash
act -W .github/workflows/perf_gate.yml
```

## Developer Admin CLI Utility (`cmd/admin`)

The `admin` CLI utility provides command-line control over sharded Redis caches and PostgreSQL entities for local debugging and environment validation.

### Compilation
Build the binary locally:
```bash
go build -o bin/admin ./cmd/admin
```

### Supported Commands

#### 1. Database Seeding (`admin db seed`)
Atomically populates the PostgreSQL database inside a single transaction with realistic test data:
- 100 Customers (pre-configured with high balances and overdraft settings).
- 100 Users (mapped to customers, with pre-computed Argon2id password hashes to speed up execution).
- 10 Advertiser Brands.
- 1000 Campaigns (fully modulated with diverse pacing modes, country targets, frequency caps, and daily budgets).

```bash
./bin/admin db seed
```

#### 2. PASETO Token Generation (`admin user create-token`)
Generates cryptographically signed PASETO tokens in memory using `TokenSymmetricKey` for rapid Curl/API testing:
```bash
./bin/admin user create-token --email user1@test.com [--auto-create]
```

#### 3. Sharded Budget Reset (`admin budget reset`)
Calculates consistent JumpHash sharding on a `CampaignID` to locate the assigned Redis node, clears edge budget buffers/sync accumulators, and optionally resets campaign `current_spend` to 0 in PostgreSQL:
```bash
./bin/admin budget reset --campaign-id <uuid> [--reset-db-spend]
```

#### 4. CRUD Entities Management
Full command suites exist to inspect and mutate relational entities:
- **Campaigns**: `admin campaign [list|get|create|delete]`
- **Customers**: `admin customer [list|get|create|update|delete]`
- **Blacklist**: `admin blacklist [list|add|delete]`
- **Users**: `admin user [list|get|create|update|delete]`

