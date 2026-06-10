# Development Guide

Requirements, tasks, and service definitions for the `eSPX` ingestion pipeline.

## Requirements

- Go 1.25+
- Docker and Docker Compose
- `buf` CLI

---

## Make Targets

| Target | Command | Purpose |
| :--- | :--- | :--- |
| `make fmt` | `go fmt ./...` | Format code. |
| `make proto` | `buf generate` | Compile Protobuf schemas. |
| `make test` | `go test -v ./...` | Run tests. |
| `make build` | `docker build ...` | Build Docker image. |

---

## Git Hooks

Git hook execution is managed via **Lefthook**.

- **Pre-commit**: Runs linter:
  ```bash
  make lint
  ```
- **Pre-push**: Runs test suite:
  ```bash
  make test
  ```

Install hooks:
```bash
lefthook install
```

---

## Ports and Services

| Service | Port | Description |
| :--- | :--- | :--- |
| **Nginx** | 8180 | Load Balancer |
| **Tracker** | 8181-8184 | Ingestion instances (`cmd/tracker.go`) |
| **Processor** | 8186 | Stream processor (`cmd/processor.go`) |
| **Management** | 8188 | Management service (`cmd/management.go`) |
| **Auth Server** | 51051 | gRPC Auth service (`cmd/auth.go`) |
| **Redis Shards** | 6479-6484 | Redis instances (0-5) |
| **PostgreSQL** | 5440 | Postgres database |
| **ClickHouse** | 9100, 8223 | ClickHouse database |
| **Prometheus** | 9190 | Telemetry scraper |
| **Alertmanager** | 9093 | Alert routing |
| **Telegram Proxy** | 8222 | Telegram webhook (`cmd/telegram.go`) |
| **Grafana** | 3100 | Visualization dashboard |

---

## CLI Tools

### DLQ Utility (`cmd/dlq.go`)

Manages the Redis Dead Letter Queue (DLQ).

*   **Archive events to disk**:
    ```bash
    go run cmd/dlq.go -action=archive -stream=ad:events:dlq -dest=dlq_archive.bin -batch=1000
    ```
    Extracts events from Redis DLQ, serializes them as length-prefixed `AdDLQEvent` Protobuf segments, writes to disk, and acknowledges the entries in Redis.
*   **Restore events from disk**:
    ```bash
    go run cmd/dlq.go -action=restore -dest=dlq_archive.bin -stream=ad:events -batch=1000 -rate=200
    ```
    Deserializes events from disk and writes them to the Redis ingestion stream (`ad:events`). The optional `-rate` parameter defines a rate limit (events/second) to prevent overwhelming the target stream; default is `0` (unlimited).
*   **Requeue directly**:
    ```bash
    go run cmd/dlq.go -action=requeue -stream=ad:events:dlq -dest=ad:events -batch=1000 -rate=500
    ```
    Moves events from the Redis DLQ to the active ingestion queue. An optional rate limit can be set via `-rate` (events/second) to control the flow.

---

## Performance Gate Setup

Pull requests are validated on target runners to ensure latency and memory budgets are met.

### Gate Thresholds

- **Heap Allocations**: `0 allocs/op`.
- **Memory Consumption**: `0 B/op`.
- **Latency Regression**: `<= 12.0%` (p < 0.05).

Compare baseline and branch benchmarks locally:
```bash
go run scripts/perf_gate.go baseline_bench.txt pr_bench.txt
```

---

## Operations and Infrastructure

### Specialized Containers

#### Dockerfile.log-evacuator
The production image built via `Dockerfile` uses a statically linked, debian-distroless base lacking a shell, package manager, and diagnostic tools. Out-of-process log evacuation requires OS-level utility bins (`rsync`, `openssh-client`, `bash`, `coreutils`). `Dockerfile.log-evacuator` packages these dependencies into a separate, isolated Alpine container to decouple file transport from the ingestion services.

### Log Evacuation Procedures
- **Hetzner Storage Box Setup**: Generate a passphrase-less Ed25519 SSH key (`~/.ssh/storagebox_id`), register the public key in Hetzner Robot Console, and set `STORAGE_BOX_SSH_KEY_PATH=/root/.ssh/storagebox_id` in `.env`.
- **Cron Evacuation Trigger**: Copy `deploy/cron/log-evacuate.cron` to `/etc/cron.d/log-evacuate` (chmod `0644`). It re-runs every 5 minutes logging execution status to `/var/log/espx-evacuate.log`.
- **Claim Renaming Details**: Active log files are compressed and encrypted directly by the logger. Rotated files are renamed with the suffix `.log.zst.ready`. The evacuator script claims these by renaming to `*.log.zst.evacuating` and uploads them via `rsync`. Local source files are deleted upon successful transfer.
- **Recovering Locked Segments**: If a crash halts upload, manually rename stuck `.evacuating` files back to `.ready` to trigger a retry.
- **Log Retention Policy**: There is no automatic retention on the Hetzner Storage Box. Manually execute purges via SSH or schedule periodic cleanups for segments older than the desired threshold.

### Redis Recovery Verification
- **AOF Load Status**: Connect to the shard and run `redis-cli INFO persistence | grep aof_enabled`. Confirm output is `aof_enabled:1`.
- **Stream Verification**: Query lengths via `redis-cli XLEN ad:events:stream` and verify active consumer groups using `redis-cli XINFO GROUPS ad:events:stream`.
- **Budget Counters**: Verify daily budget key presence with `redis-cli KEYS "budget:campaign:*"`.
- **DLQ Integrity**: Confirm DLQ stream depth using `redis-cli XLEN ad:events:dlq`.
- **Budget Reconciliation**: Realignment is initiated by running the management reconciliation worker or CLI tool, monitored via `ad_redis_reconciliation_duration_seconds`.
