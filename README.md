# Ad Event Processor

Backend system for ad event ingestion and storage.

## Features
- Ingestion via Redis Streams.
- Dual storage: PostgreSQL (transactional) and ClickHouse (analytical).
- IP rate limiting and event deduplication.
- Horizontal scalability.
- Prometheus metrics integration.

## Documentation
- [Architecture Specification](docs/architecture.md)

## Requirements
- Go 1.25+
- PostgreSQL 16
- ClickHouse 24.3
- Redis 7

## Execution
```bash
# Infrastructure
docker compose up -d

# Tests
make test

# Server
go run cmd/server/main.go
```

## Endpoints
- `POST /track`: Event ingestion.
- `GET /health`: System status.
- `GET /metrics`: Prometheus metrics.
