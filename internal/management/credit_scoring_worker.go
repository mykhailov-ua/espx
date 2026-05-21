package management

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
	"github.com/shopspring/decimal"
)

type CreditScoringWorker struct {
	svc *Service
}

func NewCreditScoringWorker(svc *Service) *CreditScoringWorker {
	return &CreditScoringWorker{svc: svc}
}

// Start spawns the background loop for dynamic credit line evaluations at specific execution intervals.
// It retrieves customer aggregated metrics from the primary database, applies the pricing multipliers, and updates overdraft tables.
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

// EvaluateAll executes a single pass over all system customers, computing and applying dynamic overdraft limits.
// It iterates through the dataset, applying time and spend multipliers to update credit metrics.
func (w *CreditScoringWorker) EvaluateAll(ctx context.Context) error {
	queries := db.New(w.svc.pool)
	rows, err := queries.ListCustomersForScoring(ctx)
	if err != nil {
		return err
	}

	for _, r := range rows {
		overdraft := w.calculateOverdraft(float64(r.AgeDays), ads.FromNumeric(r.TopupSum30d))
		customerID := uuid.UUID(r.ID.Bytes)

		cust, err := queries.GetCustomerByID(ctx, r.ID)
		if err != nil {
			slog.Error("failed to fetch customer for scoring audit", "customer_id", customerID, "error", err)
			continue
		}

		currentOverdraft := ads.FromNumeric(cust.AllowedOverdraft)
		if currentOverdraft.Equal(overdraft) {
			continue
		}

		err = w.svc.UpdateOverdraft(ctx, customerID, overdraft, currentOverdraft)
		if err != nil {
			slog.Error("failed to update overdraft for customer", "customer_id", customerID, "error", err)
		}
	}

	return nil
}

// calculateOverdraft maps payment history and account longevity to a safe allowed overdraft threshold.
// Multipliers scale based on operational duration to prevent immediate over-allocations on new profiles.
func (w *CreditScoringWorker) calculateOverdraft(ageDays float64, topupSum decimal.Decimal) decimal.Decimal {
	if ageDays < 7.0 {
		return decimal.Zero
	}

	var multiplier decimal.Decimal
	if ageDays < 30.0 {
		multiplier = decimal.NewFromFloat(0.15)
	} else {
		multiplier = decimal.NewFromFloat(0.30)
	}

	overdraft := topupSum.Mul(multiplier).Round(2)
	maxCap := decimal.NewFromInt(10000)
	if overdraft.GreaterThan(maxCap) {
		overdraft = maxCap
	}

	return overdraft
}
