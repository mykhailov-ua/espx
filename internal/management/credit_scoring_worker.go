package management

import (
	"context"
	"log/slog"
	"time"

	"espx/internal/ads/db"
	"github.com/google/uuid"
)

// CreditScoringWorker recalculates customer overdraft limits from payment history on a schedule.
type CreditScoringWorker struct {
	svc *Service
}

// NewCreditScoringWorker binds overdraft evaluation to the management service.
func NewCreditScoringWorker(svc *Service) *CreditScoringWorker {
	return &CreditScoringWorker{svc: svc}
}

// Start runs overdraft evaluation on a fixed interval until the context is cancelled.
func (w *CreditScoringWorker) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.EvaluateAll(ctx); err != nil {
				slog.Error("credit scoring evaluation failed", "error", err)
			}
		}
	}
}

// EvaluateAll recomputes and persists overdraft limits for every customer eligible for scoring.
func (w *CreditScoringWorker) EvaluateAll(ctx context.Context) error {
	opCtx, cancel := workerContext(ctx, workerBatchTimeout)
	defer cancel()

	queries := db.New(w.svc.pool)
	rows, err := queries.ListCustomersForScoring(opCtx)
	if err != nil {
		return err
	}

	for _, r := range rows {
		overdraft := w.calculateOverdraft(float64(r.AgeDays), r.TopupSum30d)
		customerID := uuid.UUID(r.ID.Bytes)

		if err := w.svc.UpdateOverdraft(opCtx, customerID, overdraft); err != nil {
			slog.Error("failed to update overdraft for customer", "customer_id", customerID, "error", err)
		}
	}

	return nil
}

// calculateOverdraft derives the allowed overdraft from account age and recent top-up volume with configured caps.
func (w *CreditScoringWorker) calculateOverdraft(ageDays float64, topupSum int64) int64 {
	if ageDays < w.svc.cfg.CreditScoringMinAgeDays {
		return 0
	}

	var overdraft int64
	if ageDays < w.svc.cfg.CreditScoringMatureAgeDays {
		overdraft = topupSum * w.svc.cfg.CreditScoringMidTierPercent / 100
	} else {
		overdraft = topupSum * w.svc.cfg.CreditScoringMaturePercent / 100
	}

	maxCap := w.svc.cfg.CreditScoringMaxCap
	if overdraft > maxCap {
		overdraft = maxCap
	}

	return overdraft
}
