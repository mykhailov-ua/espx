package management

import (
	"context"
	"log/slog"
	"time"

	"espx/internal/ads/db"
	"github.com/google/uuid"
)

type CreditScoringWorker struct {
	svc *Service
}

func NewCreditScoringWorker(svc *Service) *CreditScoringWorker {
	return &CreditScoringWorker{svc: svc}
}

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

func (w *CreditScoringWorker) EvaluateAll(ctx context.Context) error {
	queries := db.New(w.svc.pool)
	rows, err := queries.ListCustomersForScoring(ctx)
	if err != nil {
		return err
	}

	for _, r := range rows {
		overdraft := w.calculateOverdraft(float64(r.AgeDays), r.TopupSum30d)
		customerID := uuid.UUID(r.ID.Bytes)

		cust, err := queries.GetCustomerByID(ctx, r.ID)
		if err != nil {
			slog.Error("failed to fetch customer for scoring audit", "customer_id", customerID, "error", err)
			continue
		}

		currentOverdraft := cust.AllowedOverdraft
		if currentOverdraft == overdraft {
			continue
		}

		err = w.svc.UpdateOverdraft(ctx, customerID, overdraft, currentOverdraft)
		if err != nil {
			slog.Error("failed to update overdraft for customer", "customer_id", customerID, "error", err)
		}
	}

	return nil
}

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
