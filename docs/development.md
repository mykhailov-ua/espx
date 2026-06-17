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

### Local Benchmarking

Run hot-path microbenchmarks to track execution latencies and allocation behavior:
```bash
# Run all hot path benchmarks
go test -bench=BenchmarkHotPath -benchmem ./internal/ads/...

# Run audit log serialization benchmarks
go test -bench=BenchmarkHandler_auditLog -benchmem ./internal/ads/...
```

Key hot path benchmarks available:
- `BenchmarkHotPath_monotonicNano`: Measures monotonic time syscall bypass.
- `BenchmarkHotPath_latencyRingRecord`: Tracks `LatencyRing` lock-free ring recording.
- `BenchmarkHotPath_filterEngineCheck_noTimeout`: Measures CPU overhead of filter engine evaluations.
- `BenchmarkHandler_auditLog_impression_sampled`: Tracks Protobuf serialization and priority log writing under sampling rules.

Compare baseline and branch benchmarks locally:
```bash
go run scripts/perf_gate.go baseline_bench.txt pr_bench.txt
```

Verify `0 B/op` benchmarks and allocation/escape behavior locally:
```bash
go test -bench=. -benchmem ./...
```

### Compiler Optimization Analysis

Verify escape analysis results and compiler inlining decisions for hot path structures:
```bash
# Analyze stack allocation vs heap escape behavior
go build -gcflags="-m -m" ./internal/ads/... 2>&1 | grep -E "escapes to heap|escapes"

# Inspect compiler inlining decisions for low-complexity functions
go build -gcflags="-m" ./internal/ads/... 2>&1 | grep -i "inline"
```

---

## Delivery Management API Endpoints

The admin panel and management system expose endpoints for campaign lifecycle, template, and creative management:

### Campaign Templates
- `POST /admin/campaign-templates`: Creates a reusable preset configurations bundle (daily budgets, timezone, frequency caps, targeting countries, and dayparts).
- `GET /admin/campaign-templates`: Returns paginated lists of campaign presets for a specific customer tenant.
- `POST /admin/campaign-templates/{id}/instantiate`: Launches a live campaign from a preset template with overrides, guarded by a client-provided idempotency key.
- `POST /admin/campaigns/{id}/save-as-template`: Snapshots an active campaign configuration back into the templates repository.

### Campaign Delivery & Scheduling
- `POST /admin/campaigns/{id}/pause`: Operator-initiated command to pause campaign delivery.
- `POST /admin/campaigns/{id}/resume`: Operator-initiated command to reactivate campaign delivery.
- `POST /admin/campaigns/{id}/schedule`: Modifies active start/end date-time boundaries and daypart hours.

### Brand Creative Routing
- `POST /admin/brands/{id}/creatives`: Adds a weighted creative landing URL variant for user routing.
- `GET /admin/brands/{id}/creatives`: Returns creative landing URL variants configured for a brand.
- `PUT /admin/brands/{brand_id}/creatives/{id}`: Updates a creative's weights or URL values.
- `DELETE /admin/brands/{brand_id}/creatives/{id}`: Deletes a creative variant.

---

## Operations and Infrastructure

### Specialized Containers

#### Dockerfile.log-evacuator
The production image built via `Dockerfile` uses a statically linked, debian-distroless base lacking a shell, package manager, and diagnostic tools. Out-of-process log evacuation requires OS-level utility bins (`rsync`, `openssh-client`, `bash`, `coreutils`). `Dockerfile.log-evacuator` packages these dependencies into a separate, isolated Alpine container to decouple file transport from the ingestion services.

### Log Evacuation Procedures
- **Hetzner Storage Box Setup**: Generate a passphrase-less Ed25519 SSH key (`~/.ssh/storagebox_id`), register the public key in Hetzner Robot Console, and set `STORAGE_BOX_SSH_KEY_PATH=/root/.ssh/storagebox_id` in `.env`.
- **Cron Evacuation Trigger**: Copy `deploy/cron/log-evacuate.cron` to `/etc/cron.d/log-evacuate` (chmod `0644`). It re-runs every 5 minutes logging execution status to `/var/log/espx-evacuate.log`.
- **Claim Renaming Details**: Active log files are written directly as raw `.log` files. Once rotated, a dedicated background compressor worker (`StartCompressorWorker`) asynchronously compresses rotated segments with `zstd` and encrypts them with `AES-GCM` using the 12-byte incrementing nonce. The output is named `.log.zst.ready` and the original raw file is deleted. The evacuator script claims these ready files by renaming them to `*.log.zst.evacuating` and uploads them via `rsync`. Local source files are deleted upon successful transfer.
- **Recovering Locked Segments**: If a crash halts upload, manually rename stuck `.evacuating` files back to `.ready` to trigger a retry.
- **Log Retention Policy**: There is no automatic retention on the Hetzner Storage Box. Manually execute purges via SSH or schedule periodic cleanups for segments older than the desired threshold.

### TTC fail-open vs fail-closed

Time-to-click (TTC) is enforced in `unified_filter.lua` on the click path: Lua reads `imp_ts:{user}:{campaign}` set by a prior impression.

| Mode | Env | Behaviour |
| :--- | :--- | :--- |
| **Fail-open** (default) | `TTC_FAIL_CLOSED=false` | Click without `imp_ts` is accepted; `ad_ttc_bypass_total` increments (return code 10). |
| **Fail-closed** (prod) | `TTC_FAIL_CLOSED=true` | Click without `imp_ts` -> fraud (`missing_imp_ts`). Enable only after business sign-off. |

**Observability:** `ad_ttc_bypass_total`, alert `TTCBypassRateHigh` (>1% of `/track`). Before enabling fail-closed, watch bypass rate during Redis incidents.

**Geo filter:** `ad_filter_geo_duration_seconds` (sampled 1/128) for p99 MaxMind latency; alert `FilterGeoLatencyHigh` (>10us p99). Schedule/daypart stays in Go (`ScheduleFilter`) - MaxMind cannot run in Redis.

### Redis Recovery Verification
- **AOF Load Status**: Connect to the shard and run `redis-cli INFO persistence | grep aof_enabled`. Confirm output is `aof_enabled:1`.
- **Stream Verification**: Query lengths via `redis-cli XLEN ad:events:stream` and verify active consumer groups using `redis-cli XINFO GROUPS ad:events:stream`.
- **Budget Counters**: Verify daily budget key presence with `redis-cli KEYS "budget:campaign:*"`.
- **DLQ Integrity**: Confirm DLQ stream depth using `redis-cli XLEN ad:events:dlq`.
- **Budget Reconciliation**: Realignment is initiated by running the management reconciliation worker or CLI tool, monitored via `ad_redis_reconciliation_duration_seconds`.

### Redis Restart Runbook

**Problem:** after `SCRIPT FLUSH`, Redis restart, or shard failover the in-memory Lua script cache is empty. Running trackers keep a cached EVALSHA digest and set `ad_redis_lua_script_loaded=1` only at startup - the gauge can stay stale while Redis has no script. Hot path falls back to full `EVAL` (see `ad_redis_lua_noscript_total`). Volatile `budget:campaign:*` keys are restored from AOF on a normal restart, but are absent after `FLUSHDB`, volume loss, or TTL expiry (24h).

**Related alerts:** `RedisLuaNoScriptFallback`, `RedisLuaScriptNotLoaded`, `BudgetCacheMissPG`, `BudgetCacheMissRatioHigh`.

#### Restart order (planned maintenance)

1. Restart Redis shards (`redis-0` ... `redis-5`) and wait until `redis-cli PING` succeeds on every port (`6479`-`6484`).
2. Verify AOF replay: `redis-cli INFO persistence | grep aof_load`.
3. **Rolling restart trackers** (`tracker-0` ... `tracker-3`) one instance at a time. Each startup runs `PreloadScripts` (SCRIPT LOAD per shard) and `WarmFromRegistry` (SET NX for missing budget keys).
4. Confirm recovery:
   - `ad_redis_lua_script_loaded{shard}` == 1 on all shards
   - `rate(ad_redis_lua_noscript_total[5m])` == 0
   - `rate(ad_budget_cache_miss_pg_total[5m])` == 0 under load

```bash
# Example (docker compose, host network)
for t in tracker-0 tracker-1 tracker-2 tracker-3; do
  docker compose restart "$t"
  sleep 30   # drain via nginx upstream before next instance
done
```

#### Recovery without tracker restart (emergency)

Use when rolling restart is not immediately possible. Restores Lua on Redis; budget keys repopulate via registry sync (SET NX, existing keys untouched).

**1. Manual SCRIPT LOAD on every shard**

Script body is embedded in the tracker binary (`internal/ads/unified_filter.lua`). SHA is deterministic - same body yields the same digest the tracker already caches.

```bash
export REDIS_PASSWORD='...'   # from .env
LUA_FILE=internal/ads/unified_filter.lua

for port in 6479 6480 6481 6482 6483 6484; do
  sha=$(redis-cli -p "$port" -a "$REDIS_PASSWORD" --no-auth-warning SCRIPT LOAD "$(cat "$LUA_FILE")")
  echo "shard port=$port sha=$sha"
  redis-cli -p "$port" -a "$REDIS_PASSWORD" --no-auth-warning SCRIPT EXISTS "$sha"
done
```

**2. Trigger budget cache warm**

Any valid campaign UUID on `campaigns:update` debounces a full registry `Sync` -> `warmBudgetCache` (SET NX). Or wait for `REGISTRY_SYNC_INTERVAL_MS` (default 60s).

```bash
# Pick any active campaign UUID from management UI or Postgres
redis-cli -p 6479 -a "$REDIS_PASSWORD" --no-auth-warning \
  PUBLISH campaigns:update "00000000-0000-0000-0000-000000000001"
```

Watch `ad_budget_cache_warm_total` and `ad_budget_cache_miss_total` in Grafana.

**3. Verify**

```bash
curl -s localhost:8181/metrics | grep -E 'ad_redis_lua_noscript_total|ad_redis_lua_script_loaded|ad_budget_cache_miss'
```

`ad_redis_lua_script_loaded` updates only after tracker `PreloadScripts` - manual SCRIPT LOAD stops NOSCRIPT fallbacks but may not clear that gauge. Prefer rolling restart when the `RedisLuaScriptNotLoaded` alert is firing.

#### On-call decision tree

| Alert | Immediate action | Proper fix |
| :--- | :--- | :--- |
| `RedisLuaNoScriptFallback` | Manual SCRIPT LOAD on affected shard(s) | Rolling restart all trackers |
| `RedisLuaScriptNotLoaded` | Rolling restart trackers (failed startup preload) | Fix Redis connectivity, redeploy |
| `BudgetCacheMissPG` | PUBLISH `campaigns:update` or wait for sync tick | Rolling restart if keys broadly missing |

**Never** run `SCRIPT FLUSH` or `FLUSHDB` in production without a maintenance window and this runbook.

## P2-10 Multi-shard operability runbook

Shard mapping: client-side (tracker + warmer + mgmt). tracker uses `StaticSlotSharder` (O(1) 1024 slots, fixed N in prod). See `internal/ads/sharding.go` and `sharding_test.go:TestSharderRebalanceImpact` for Static vs JumpHash remap stats (~100% keys move on N change for static vs ~1/N for jump).

### Shard down / failover (blast radius)
- Symptom: `ad_redis_breaker_state{shard="X"} == 1` (open), or `/health` on :8181 reports `DEGRADED redis=...X:0`, or per-shard health on :9090/metrics? no, use /health body.
- Effect: campaigns hashing to that shard get 503 + Retry-After:1 from infra/breaker (see handler filter + breaker hook). Other shards unaffected (key property of client sharding).
- Mitigation (immediate):
  1. Confirm which campaigns affected: use mgmt API or query PG for active, compute shard = StaticSlot( crc32(camp.ID) & 1023 ) % N ; or from logs.
  2. If transient (net/restart): wait breaker half-open (default 5s). Budget keys are in that redis only.
  3. If permanent loss: fail over the campaigns by changing effective shard count? No - for static, to move load: update infra LB or deploy with temp override? Current: no dynamic reshard in hot path.
- Long term: budget key migration (below).

### Budget key migration between shards (rebalance or failover recovery)
Budget keys live only on their hashed shard: `budget:campaign:UUID` , `budget:daily_spent:...` , fcap etc. Lua operates on single shard.
To move a campaign from shard S to T (e.g. after adding node or bad shard):
1. Pause delivery for campaign (set status PAUSED in mgmt, or use emergency breaker).
2. On source shard S: DUMP or redis-cli --rdb extract the keys for the camp (or use MIGRATE / DUMP+RESTORE).
   Example: redis-cli -h shardS --eval - <<'LUA' 0 "budget:campaign:$ID" "budget:daily*"
   -- custom dump script or use redis-migrate-tool / app script.
3. On target T: RESTORE the keys with TTLs preserved. For daily_spent keys (short TTL) can warm via registry instead.
4. Verify: from tracker pod, `redis-cli -h target GET "budget:campaign:$ID"` returns the remaining.
5. Update registry? No - sharding is pure hash(id) % N , changing N or slots requires all clients agree (trackers + any mgmt workers using sharder).
   To change N live: blue/green deploy trackers with new StaticSlot(N'), simultaneously migrate all keys for affected camps, then cut traffic. High risk.
   Prefer JumpHashSharder if frequent rebalance expected (see comparison in sharding.go).
6. Resume campaign. Monitor `ad_budget_cache_miss_pg_total` and Lua p99 during transition.
7. For daily budgets: they can be re-warmed from PG via registry sync + warmer after keys present on new shard.

### Health per shard
- Main port /health (gnet): returns "OK redis=0:1,1:1,..." or "DEGRADED ...". status 200/503.
- Background probe every 2s populates atomics (no ping on /health path).
- Sidecar :9090/health always 200 for scraper.
- Update k8s readiness if needed to drain on partial shard loss (outside this repo).

### When to prefer Jump vs Static
Run `go test ./internal/ads/ -run TestSharderRebalanceImpact -v` after changes. Static wins on fixed cluster + DOD (no branches, no float, fits L1). Jump for autoscaling shards.

See also: prometheus alert WorkerPoolReject, breaker alerts, per-shard in /health body.
