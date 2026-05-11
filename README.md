# Ad Event Processor

Backend system for high-throughput ad event ingestion, processing, and storage.

## Features
- Ingestion via Redis Streams with Protobuf serialization.
- Decoupled architecture: independent Ingestion Server (Tracker) and Stream Processor.
- Dual storage: PostgreSQL (transactional/aggregates) and ClickHouse (analytical logs).
- Reliability: Circuit Breaker for database protection and Dead Letter Queue (DLQ) for failed events.
- Budget Management: Atomic reservation via Redis Lua scripts with asynchronous PostgreSQL synchronization.
- Observability: Prometheus metrics and Grafana dashboards.

## Requirements
- Go 1.25+
- PostgreSQL 16
- ClickHouse 24.3
- Redis 7

## Execution

### Docker Deployment
The system is deployed as two distinct services sharing the same image but different entrypoints.
```bash
# Infrastructure and Services
docker compose up -d
```

### Manual Execution
```bash
# Ingestion Server (Tracker)
go run cmd/server/main.go

# Stream Processor
go run cmd/processor/main.go
```

## Endpoints

### Tracker (Ingestion)
- `POST /track`: Receives ad events (JSON/Protobuf).
- `GET /health`: Health status.

### Processor (Observability)
- `GET /health`: Health status.
- `GET /metrics`: Prometheus metrics (includes DLQ and Circuit Breaker states).
