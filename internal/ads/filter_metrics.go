package ads

import (
	"sync/atomic"

	"espx/internal/metrics"
)

// Pre-bound filter error counters avoid Prometheus label lookup on the rejection hot path.
var (
	filterGeoLookupErrors        = metrics.FilterInternalErrors.WithLabelValues("geo_lookup")
	filterFraudStreamWriteErrors = metrics.FilterInternalErrors.WithLabelValues("fraud_stream_write")
	filterEngineFailures         = metrics.FilterInternalErrors.WithLabelValues("filter_engine")
	filterGeoDuration            = metrics.FilterGeoDuration
	geoMetricsSeq                atomic.Uint64
)
