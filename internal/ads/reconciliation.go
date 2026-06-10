package ads

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"espx/internal/domain"
	"espx/internal/metrics"
)

// lag avoids comparing against partially-flushed ClickHouse batches.
type ReconciliationWorker struct {
	pgConn     PostgresConn
	chConn     ClickHouseConn
	repo       domain.CampaignRepository
	driftLimit float64
	lag        time.Duration
	interval   time.Duration
}

func NewReconciliationWorker(
	pg PostgresConn,
	ch ClickHouseConn,
	repo domain.CampaignRepository,
	driftLimit float64,
	lag time.Duration,
	interval time.Duration,
) *ReconciliationWorker {
	return &ReconciliationWorker{
		pgConn:     pg,
		chConn:     ch,
		repo:       repo,
		driftLimit: driftLimit,
		lag:        lag,
		interval:   interval,
	}
}

func (rw *ReconciliationWorker) Reconcile(ctx context.Context) error {
	campaigns, err := rw.repo.ListActive(ctx)
	if err != nil {
		return fmt.Errorf("reconciliation failed to list active campaigns: %w", err)
	}

	if len(campaigns) == 0 {
		return nil
	}

	until := time.Now().Add(-rw.lag)
	chSpends, err := rw.chConn.QueryAggregatedSpend(ctx, until)
	if err != nil {
		return fmt.Errorf("reconciliation failed to query ClickHouse aggregates: %w", err)
	}

	for _, c := range campaigns {
		pgSpend, err := rw.pgConn.GetCampaignSpend(ctx, c.ID)
		if err != nil {
			slog.Error("Reconciliation: failed to get Postgres spend", "campaign_id", c.ID, "error", err)
			continue
		}

		chSpend := chSpends[c.ID]

		var drift float64
		if pgSpend > 0 {
			drift = math.Abs(float64(pgSpend-chSpend)) / float64(pgSpend)
		} else if chSpend > 0 {
			drift = 1.0
		}

		metrics.DataDriftRatio.WithLabelValues(c.ID.String()).Set(drift)

		if drift > rw.driftLimit {
			slog.Warn("Reconciliation: CRITICAL DATA DRIFT DETECTED",
				"campaign_id", c.ID,
				"pg_spend", pgSpend,
				"ch_spend", chSpend,
				"drift_ratio", drift,
				"limit", rw.driftLimit,
			)
		} else {
			slog.Info("Reconciliation: campaign balances within normal drift limits",
				"campaign_id", c.ID,
				"pg_spend", pgSpend,
				"ch_spend", chSpend,
				"drift_ratio", drift,
			)
		}
	}

	return nil
}

func (rw *ReconciliationWorker) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(rw.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := rw.Reconcile(ctx); err != nil {
					slog.Error("Reconciliation: loop execution error", "error", err)
				}
			}
		}
	}()
}
