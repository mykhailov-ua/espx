# Development Guide

Tooling, testing, and maintenance workflow.

## Requirements
- Go 1.25+
- Docker & Docker Compose

## Makefile Targets

| Target | Action |
| :--- | :--- |
| `make fmt` | Format code via `gofmt`. |
| `make lint` | Run `golangci-lint` with `.golangci.yml`. |
| `make test` | Run all tests (Unit + Integration). |
| `make test-unit` | Run fast unit tests. |
| `make test-int` | Run integration tests (requires Docker). |
| `make build` | Build production Docker image. |

## Local Infrastructure
Spin up full stack (Postgres, Redis, Prometheus, Grafana):
```bash
docker compose up -d
```

## Testing

### Unit
- `tests/unit/`
- Isolated logic testing. Uses **Testcontainers** for fast, ephemeral Redis/Postgres testing when needed.

### Integration
- `tests/integration/`
- End-to-end flow validation using real containers.
- Covers: Graceful Shutdown, Stream recovery, and SQL CTE aggregation accuracy.

## CI/CD
GitHub Actions workflow:
1. **Lint**: Style and static analysis.
2. **Test**: Full suite (Unit + Integration).
3. **Build**: Docker build validation.

## Metrics & UI
- **Prometheus**: `http://localhost:9095`
- **Grafana**: `http://localhost:3005` (admin/admin)
- **pprof**: `http://localhost:8085/debug/pprof/` (if SERVER_PORT is 8085)
