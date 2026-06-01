// Package metrics declares all Prometheus metrics for the eSPX pipeline.
// Metrics are registered at package initialisation via promauto (no explicit
// registration call required). All counter and histogram names follow the
// ad_<subsystem>_<metric> convention:
//
//	ad_http_*           HTTP layer counters/histograms (tracker + net/http path).
//	ad_events_*         Stream ingestion throughput and drop counters.
//	ad_filter_*         Filter engine decisions and blocked-event breakdown.
//	ad_db_*             PostgreSQL and ClickHouse write latency and error counts.
//	ad_circuit_breaker_* Circuit breaker state gauge (0=closed, 1=open, 2=half-open).
//	ad_dlq_*            Dead-letter queue depth.
//	ad_management_*     Management service business metrics (commissions, top-ups).
//	ad_reconciliation_* Data-drift gauges and correction counters.
//	ad_gnet_*           gnet event-loop counters (packets, bytes, connections).
//	ad_redis_lua_*      Redis Lua script execution duration histogram.
//	ad_registry_*       Campaign registry sync lag.
//	ad_uuid_*           NewFastUUID generation duration (nanosecond-scale buckets).
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HttpRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_http_requests_total",
		Help: "Total number of HTTP requests by status code",
	}, []string{"method", "path", "status"})

	HttpRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ad_http_request_duration_seconds",
		Help:    "Latency of HTTP requests in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "path"})

	EventsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_events_processed_total",
		Help: "Total number of events successfully accepted into Redis Streams",
	})

	EventsDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_events_dropped_total",
		Help: "Total number of events dropped due to Redis ingestion failure",
	})

	FilterBlockedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_filter_blocked_total",
		Help: "Total number of events blocked by filters",
	}, []string{"reason"})

	DbWriteDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ad_db_write_duration_seconds",
		Help:    "Duration of database batch write operations",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5},
	}, []string{"type"})

	DbWriteErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_db_write_errors_total",
		Help: "Total number of database write errors",
	}, []string{"type"})

	CircuitBreakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ad_circuit_breaker_state",
		Help: "Current state of the circuit breaker (0=closed, 1=open, 2=half-open)",
	}, []string{"group"})

	DlqSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ad_dlq_size_total",
		Help: "Current number of events in the Dead Letter Queue",
	})

	CommissionsCollectedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_management_commissions_total",
		Help: "Total amount of commissions collected from campaign cancellations",
	})

	BalanceTopupsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_management_topups_total",
		Help: "Total amount of customer balance top-ups",
	}, []string{"currency"})

	ActiveCampaigns = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ad_management_active_campaigns_count",
		Help: "Current number of active campaigns in the system",
	})

	DataDriftRatio = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ad_reconciliation_drift_ratio",
		Help: "Ratio of discrepancy between Postgres and ClickHouse spend",
	}, []string{"campaign_id"})

	ReconRunsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_reconciliation_runs_total",
		Help: "Total number of completed reconciliation runs",
	}, []string{"status"})

	ReconDiscrepanciesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_reconciliation_discrepancies_total",
		Help: "Total number of campaign discrepancies found",
	})

	ReconTotalDelta = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_reconciliation_total_delta_micro_units",
		Help: "Absolute net discrepancy corrected by reconciliation in micro units",
	})

	ReconAdjustmentErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_reconciliation_adjustment_errors_total",
		Help: "Total number of errors during automated reconciliation corrections",
	})

	GnetPacketsReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_gnet_packets_received_total",
		Help: "Total number of network packets received",
	})
	GnetPacketsSent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_gnet_packets_sent_total",
		Help: "Total number of network packets sent",
	})
	GnetActiveConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ad_gnet_active_connections",
		Help: "Current number of active TCP connections",
	})
	GnetEventLoopWorkDuration = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_gnet_event_loop_work_duration_seconds_total",
		Help: "Total execution time spent doing active processing in gnet event loops",
	})
	GnetBytesReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_gnet_bytes_received_total",
		Help: "Total number of bytes received via gnet",
	})
	GnetBytesSent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_gnet_bytes_sent_total",
		Help: "Total number of bytes sent via gnet",
	})
	HttpParseErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_http_parse_errors_total",
		Help: "Total number of HTTP/1.1 parsing errors",
	}, []string{"error_type"})

	FilterThroughput = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_filter_throughput_total",
		Help: "Total throughput through the filter engine",
	}, []string{"format"})
	FilterDecisions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_filter_decisions_total",
		Help: "Filter decisions made by the engine",
	}, []string{"decision"})
	RedisLuaDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ad_redis_lua_duration_seconds",
		Help:    "Execution duration of Redis Lua filters",
		Buckets: []float64{0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
	}, []string{"shard"})
	RegistrySyncLag = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ad_registry_sync_lag_seconds",
		Help:    "Registry sync lag between database update and cache loading",
		Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
	})
	UuidGenDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ad_uuid_generation_duration_nanoseconds",
		Help:    "Execution duration of NewFastUUID in nanoseconds",
		Buckets: []float64{10, 25, 50, 100, 250, 500, 1000, 2500, 5000},
	})
	GeoProviderStatus = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ad_geo_provider_status",
		Help: "Status of the geo provider: 1 = real MaxMind, 0 = mock",
	})
)
