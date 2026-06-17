package management

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"espx/internal/ads/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// CampaignDrainWorker finalizes cancelled campaigns after the hot path drain window elapses.
type CampaignDrainWorker struct {
	svc *Service
}

// NewCampaignDrainWorker binds campaign drain finalization to the management service.
func NewCampaignDrainWorker(svc *Service) *CampaignDrainWorker {
	return &CampaignDrainWorker{svc: svc}
}

// Start polls for draining campaigns ready to finalize until the context is cancelled.
func (w *CampaignDrainWorker) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.ProcessDraining(ctx); err != nil {
				if strings.Contains(err.Error(), "closed pool") {
					return
				}
				slog.Error("failed to process draining campaigns", "error", err)
			}
		}
	}
}

// ProcessDraining finalizes up to one hundred draining campaigns per tick within a bounded timeout.
func (w *CampaignDrainWorker) ProcessDraining(ctx context.Context) error {
	opCtx, cancel := workerContext(ctx, workerDrainTimeout)
	defer cancel()

	waitTimeoutMs := int64(100)
	if w.svc.cfg != nil && w.svc.cfg.Lifecycle.WaitTimeoutMs > 0 {
		waitTimeoutMs = int64(w.svc.cfg.Lifecycle.WaitTimeoutMs)
	}
	threshold := time.Now().Add(-time.Duration(waitTimeoutMs) * time.Millisecond)

	for i := 0; i < 100; i++ {
		finalized, err := w.finalizeNextDraining(opCtx, threshold)
		if err != nil {
			return err
		}
		if !finalized {
			return nil
		}
	}
	return nil
}

// finalizeNextDraining locks and completes one draining campaign that has passed the wait threshold.
func (w *CampaignDrainWorker) finalizeNextDraining(ctx context.Context, threshold time.Time) (bool, error) {
	finalized := false
	err := pgx.BeginFunc(ctx, w.svc.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		camps, err := q.GetDrainingCampaignsForUpdate(ctx, db.GetDrainingCampaignsForUpdateParams{
			UpdatedAt: pgtype.Timestamptz{Time: threshold, Valid: true},
			Limit:     1,
		})
		if err != nil {
			return err
		}
		if len(camps) == 0 {
			return nil
		}
		camp := camps[0]
		campaignID := uuid.UUID(camp.ID.Bytes)
		if err := w.svc.finalizeDrainingCampaign(ctx, q, campaignID, camp, "Finalized"); err != nil {
			return err
		}
		finalized = true
		return nil
	})
	return finalized, err
}
