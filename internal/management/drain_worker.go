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

type CampaignDrainWorker struct {
	svc *Service
}

func NewCampaignDrainWorker(svc *Service) *CampaignDrainWorker {
	return &CampaignDrainWorker{svc: svc}
}

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

func (w *CampaignDrainWorker) ProcessDraining(ctx context.Context) error {
	waitTimeoutMs := int64(100)
	if w.svc.cfg != nil && w.svc.cfg.Lifecycle.WaitTimeoutMs > 0 {
		waitTimeoutMs = int64(w.svc.cfg.Lifecycle.WaitTimeoutMs)
	}

	threshold := time.Now().Add(-time.Duration(waitTimeoutMs) * time.Millisecond)

	var campaignIDs []uuid.UUID
	err := pgx.BeginFunc(ctx, w.svc.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		camps, err := q.GetDrainingCampaignsForUpdate(ctx, db.GetDrainingCampaignsForUpdateParams{
			UpdatedAt: pgtype.Timestamptz{Time: threshold, Valid: true},
			Limit:     100,
		})
		if err != nil {
			return err
		}
		for _, camp := range camps {
			campaignIDs = append(campaignIDs, uuid.UUID(camp.ID.Bytes))
		}
		return nil
	})
	if err != nil {
		return err
	}

	for _, id := range campaignIDs {
		if err := w.svc.FinalizeCancelledCampaign(ctx, id, "Finalized"); err != nil {
			slog.Error("failed to finalize cancelled campaign", "campaign_id", id, "error", err)
		}
	}
	return nil
}
