package management

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/metrics"
	redis "github.com/redis/go-redis/v9"
)

// ReconService performs financial reconciliation between the authoritative PostgreSQL ledger
// and the edge state in Redis (plus secondary volume signals from ClickHouse).
// It is deliberately a cold-path background process running on closed time windows to avoid
// any contention with the hot SyncWorker / Processor settlement paths.
type ReconService struct {
	pool *pgxpool.Pool
	rdb  redis.UniversalClient
}

// NewReconService constructs the reconciliation engine.
// Why: centralizes all delta detection and corrective action in one place with explicit
// dependencies, making the cold-path logic easy to test in isolation and reason about.
func NewReconService(pool *pgxpool.Pool, rdb redis.UniversalClient) *ReconService {
	return &ReconService{pool: pool, rdb: rdb}
}

// ReconcileWindow is the core entry point. It is called by the background ReconWorker
// for a deliberately "closed" hour (e.g. two hours in the past).
// The method is idempotent per (period_start, period_end) thanks to the unique run record.
func (s *ReconService) ReconcileWindow(ctx context.Context, start, end time.Time) error {
	run, err := s.createRun(ctx, start, end)
	if err != nil {
		slog.Error("failed to create recon run record", "error", err, "start", start, "end", end)
		metrics.ReconRunsTotal.WithLabelValues("failed").Inc()
		return err
	}

	// Collect authoritative spend from ledger for the closed window.
	// We sum only negative movements that represent actual budget consumption.
	ledgerRows, err := s.pool.Query(ctx, `
		SELECT campaign_id, COALESCE(SUM(CASE WHEN amount < 0 THEN -amount ELSE 0 END), 0)::bigint
		FROM balance_ledger
		WHERE created_at >= $1 AND created_at < $2
		  AND (type IN ('FEE', 'RECONCILIATION_ADJUST', 'REFUND'))
		GROUP BY campaign_id
	`, start, end)
	if err != nil {
		s.failRun(ctx, run.ID, err)
		metrics.ReconRunsTotal.WithLabelValues("failed").Inc()
		return err
	}
	defer ledgerRows.Close()

	ledgerMap := make(map[uuid.UUID]int64)
	for ledgerRows.Next() {
		var cid uuid.UUID
		var spent int64
		if err := ledgerRows.Scan(&cid, &spent); err != nil {
			slog.Error("failed to scan ledger row in recon run", "run_id", run.ID, "error", err)
			continue
		}
		ledgerMap[cid] = spent
	}

	// For each campaign that had activity, fetch the current "truth" from Redis sync keys.
	// The sync keys represent what the edge has actually reserved and what has been
	// acknowledged as settled by the SyncWorker.
	// We compare ledger cumulative vs Redis sync state.
	discrepancies := 0
	var totalDelta int64

	for campID, ledgerSpent := range ledgerMap {
		// Redis keys maintained by unified_filter.lua + sync_worker.go
		syncKey := "budget:sync:campaign:" + campID.String()
		// The sync value is the amount that has been moved from "current" to "sync" bucket.
		syncVal, err := s.rdb.Get(ctx, syncKey).Int64()
		if err != nil && !errors.Is(err, redis.Nil) {
			slog.Error("failed to fetch campaign sync budget from Redis in recon", "campaign_id", campID, "error", err)
			metrics.ReconAdjustmentErrors.Inc()
			s.failRun(ctx, run.ID, err)
			metrics.ReconRunsTotal.WithLabelValues("failed").Inc()
			return err
		}

		// Expected from edge perspective (what was actually consumed at the edge)
		expected := syncVal
		actual := ledgerSpent
		delta := expected - actual // positive = ledger under-charged the customer

		if delta == 0 {
			continue
		}

		discrepancies++

		// Record the finding to postgres.
		_, err = s.pool.Exec(ctx, `
			INSERT INTO recon_discrepancies (run_id, campaign_id, customer_id, expected_spend, actual_spend, delta, redis_adjusted)
			VALUES ($1, $2, $3, $4, $5, $6, false)
		`, run.ID, ads.ToUUID(campID), ads.ToUUID(uuid.Nil), expected, actual, delta)
		if err != nil {
			slog.Error("failed to record recon discrepancy to postgres", "run_id", run.ID, "campaign_id", campID, "error", err)
			metrics.ReconAdjustmentErrors.Inc()
			s.failRun(ctx, run.ID, err)
			metrics.ReconRunsTotal.WithLabelValues("failed").Inc()
			return err
		}

		// Perform atomic correction on Redis if the delta is material and safe.
		// The Lua script below guarantees atomicity even under concurrent SyncWorker activity.
		if err := s.adjustRedisBudgetAtomically(ctx, campID, delta); err != nil {
			slog.Error("failed to adjust Redis budget atomically in recon", "campaign_id", campID, "delta", delta, "error", err)
			metrics.ReconAdjustmentErrors.Inc()
			// Continue to next campaign but track error
			continue
		}

		// Mark as adjusted and also create a corrective ledger entry for audit.
		adjType := "RECONCILIATION_ADJUST"
		_, err = s.pool.Exec(ctx, `
			INSERT INTO balance_ledger (customer_id, campaign_id, amount, type, created_at)
			VALUES ($1, $2, $3, $4, NOW())
		`, ads.ToUUID(uuid.Nil), ads.ToUUID(campID), -delta, adjType) // negative delta means credit to customer
		if err != nil {
			slog.Error("failed to insert corrective ledger entry for recon", "campaign_id", campID, "delta", delta, "error", err)
			metrics.ReconAdjustmentErrors.Inc()
			continue
		}

		_, err = s.pool.Exec(ctx, `
			UPDATE recon_discrepancies 
			SET redis_adjusted = true 
			WHERE run_id = $1 AND campaign_id = $2
		`, run.ID, ads.ToUUID(campID))
		if err != nil {
			slog.Error("failed to mark recon discrepancy as adjusted in postgres", "run_id", run.ID, "campaign_id", campID, "error", err)
			metrics.ReconAdjustmentErrors.Inc()
		}

		totalDelta += delta
	}

	// Finalize the run record.
	_, err = s.pool.Exec(ctx, `
		UPDATE recon_runs 
		SET status = 'COMPLETED', total_delta = $1, campaigns_checked = $2, discrepancies_found = $3, completed_at = NOW()
		WHERE id = $4
	`, totalDelta, len(ledgerMap), discrepancies, run.ID)
	if err != nil {
		slog.Error("failed to finalize recon run in postgres", "run_id", run.ID, "error", err)
		metrics.ReconRunsTotal.WithLabelValues("failed").Inc()
		return err
	}

	// Update Prometheus metrics to power alarms and dashboards.
	metrics.ReconRunsTotal.WithLabelValues("success").Inc()
	if discrepancies > 0 {
		metrics.ReconDiscrepanciesTotal.Add(float64(discrepancies))
	}
	metrics.ReconTotalDelta.Add(float64(abs(totalDelta)))

	slog.Info("reconciliation completed",
		"period", start.Format(time.RFC3339)+"-"+end.Format(time.RFC3339),
		"delta", totalDelta,
		"discrepancies", discrepancies,
	)
	return nil
}

// adjustRedisBudgetAtomically uses a Lua script to safely correct the sync counters.
// Why: INCRBY is atomic, but we must also protect against the key being deleted by the
// normal SyncWorker in the same millisecond. The script returns the new remaining value.
func (s *ReconService) adjustRedisBudgetAtomically(ctx context.Context, campID uuid.UUID, delta int64) error {
	script := `
		local key = KEYS[1]
		local delta = tonumber(ARGV[1])
		local current = redis.call("GET", key)
		if not current then current = "0" end
		local newVal = tonumber(current) + delta
		if newVal <= 0 then
			redis.call("DEL", key)
			return 0
		end
		redis.call("SET", key, tostring(newVal))
		return newVal
	`
	_, err := s.rdb.Eval(ctx, script, []string{"budget:sync:campaign:" + campID.String()}, delta).Result()
	return err
}

func (s *ReconService) createRun(ctx context.Context, start, end time.Time) (struct{ ID int64 }, error) {
	var run struct{ ID int64 }
	err := s.pool.QueryRow(ctx, `
		INSERT INTO recon_runs (period_start, period_end, status) VALUES ($1, $2, 'PENDING') RETURNING id
	`, start, end).Scan(&run.ID)
	return run, err
}

func (s *ReconService) failRun(ctx context.Context, id int64, err error) {
	_, execErr := s.pool.Exec(ctx, `UPDATE recon_runs SET status = 'FAILED' WHERE id = $1`, id)
	if execErr != nil {
		slog.Error("failed to mark recon run status as failed in postgres", "run_id", id, "error", execErr)
	}
	slog.Error("reconciliation run failed", "run_id", id, "error", err)
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
