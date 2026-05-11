# Development Guide

Tooling, testing, and maintenance workflow.

## Requirements
- Go 1.25+
- Docker & Docker Compose
- `buf` (for Protobuf generation)

## Makefile Targets

| Target | Action |
| :--- | :--- |
| `make fmt` | Format code via `go fmt`. |
| `make lint` | Run `golangci-lint` for static analysis. |
| `make test` | Run all tests (Unit + Integration). |
| `make test-unit` | Run fast unit tests (`internal/...`). |
| `make test-int` | Run integration tests (`tests/...`). |
| `make build` | Build production Docker image (contains all binaries). |
| `make proto` | Generate Go code from Protobuf definitions using `buf`. |

## Local Infrastructure
Spin up full stack (Postgres, Redis, ClickHouse, Prometheus, Grafana):
```bash
docker compose up -d
```

## Testing

### Unit Tests
- Location: `internal/...`
- Purpose: Logic validation for isolated packages.
- Execution: `make test-unit`

### Integration Tests
- Location: `tests/`
- Purpose: End-to-end flow validation requiring real infrastructure.
- Scenarios:
    - `e2e_test.go`: Full ingestion-to-storage flow.
    - `shutdown_test.go`: Graceful stop and drain logic.
    - `budget_integration_test.go`: Redis/Postgres budget reconciliation.
    - `circuit_breaker_integration_test.go`: Behavior during database outages.

## CI/CD
GitHub Actions workflow performs:
1. **Lint**: Code style and static analysis.
2. **Test**: Concurrent execution of unit and integration suites.
3. **Build**: Multi-stage Docker build validation.

## Observability
- **Prometheus**: `http://localhost:9095` (Scrapes `tracker` and `processor`).
- **Grafana**: `http://localhost:3005` (Provisioned with dashboards).
- **pprof**: Enabled via environment variables on specific ports.
