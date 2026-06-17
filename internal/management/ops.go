package management

import (
	"context"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

// RegisterOpsRoutes mounts unauthenticated health and metrics endpoints for orchestration probes.
func RegisterOpsRoutes(mux *http.ServeMux, pool *pgxpool.Pool, rdbs []redis.UniversalClient) {
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		if err := pool.Ping(ctx); err != nil {
			http.Error(w, "postgres unreachable", http.StatusServiceUnavailable)
			return
		}
		for i, rdb := range rdbs {
			if err := rdb.Ping(ctx).Err(); err != nil {
				http.Error(w, "redis shard unreachable", http.StatusServiceUnavailable)
				_ = i
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	mux.Handle("GET /metrics", promhttp.Handler())
}
