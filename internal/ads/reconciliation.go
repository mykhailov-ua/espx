// Package ads implements ReconciliationWorker, a periodic data-integrity agent that
// compares PostgreSQL spend totals against ClickHouse aggregated event volumes to
// detect financial drift. Drift is defined as:
//
//	abs(pgSpend - chSpend) / pgSpend
//
// If drift exceeds driftLimit, a structured CRITICAL warning is emitted and the
// ad_reconciliation_drift_ratio gauge is updated. The ClickHouse query uses a lag
// offset (typically 5-10 minutes) to account for batched writes and replication lag
// before the aggregates are considered stable.
//
// ReconciliationWorker does not auto-correct spend totals; it is a read-only
// diagnostic tool. Financial corrections are performed by SnapshotReplicator.
package ads

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/mykhailov-ua/ad-event-processor/internal/metrics"
)

// ReconciliationWorker compares PostgreSQL and ClickHouse spend per active campaign.
// driftLimit is the fractional threshold above which a CRITICAL log is emitted.
// lag is subtracted from time.Now() before querying ClickHouse to avoid reading
// partially-flushed batches that would inflate the apparent drift.
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

// Reconcile runs one reconciliation pass across all active campaigns. Each campaign
// gets an individual Prometheus gauge update; campaigns with drift > driftLimit
// trigger a WARN log with pg_spend, ch_spend, and drift_ratio fields.
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
