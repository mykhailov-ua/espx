package management

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
)

// AutoscaleBudgets coordinates budget shifts from low CTR to high CTR campaigns under the same customer.
// It executes a synchronous SyncAll over all SyncWorkers to ensure Postgres data is fully current.
func (s *Service) AutoscaleBudgets(ctx context.Context, syncWorkers []*ads.SyncWorker) error {
	for _, sw := range syncWorkers {
		sw.SyncAll(ctx)
	}

	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		rows, err := q.GetAllActiveCampaignsWithStats(ctx)
		if err != nil {
			return fmt.Errorf("failed to fetch active campaigns with stats: %w", err)
		}

		byCustomer := make(map[uuid.UUID][]db.GetAllActiveCampaignsWithStatsRow)
		for _, row := range rows {
			custID := uuid.UUID(row.CustomerID.Bytes)
			byCustomer[custID] = append(byCustomer[custID], row)
		}

		for custID, campaigns := range byCustomer {
			if len(campaigns) < 2 {
				continue
			}

			var bestCamp *db.GetAllActiveCampaignsWithStatsRow
			var bestCTR float64 = -1.0

			var worstCamp *db.GetAllActiveCampaignsWithStatsRow
			var worstCTR float64 = 2.0

			for i := range campaigns {
				c := &campaigns[i]
				if c.TotalImpressions <= 0 {
					continue
				}
				ctr := float64(c.TotalClicks) / float64(c.TotalImpressions)

				if ctr > s.cfg.AutoscaleHighCTRThreshold && c.TotalImpressions > s.cfg.AutoscaleMinImpressions {
					if ctr > bestCTR {
						bestCTR = ctr
						bestCamp = c
					}
				}

				limit := c.BudgetLimit
				spend := c.CurrentSpend
				remaining := limit - spend

				if ctr < s.cfg.AutoscaleLowCTRThreshold && remaining >= s.cfg.AutoscaleMinRemainingBudget {
					if ctr < worstCTR {
						worstCTR = ctr
						worstCamp = c
					}
				}
			}

			if bestCamp != nil && worstCamp != nil {
				bestID := uuid.UUID(bestCamp.ID.Bytes)
				worstID := uuid.UUID(worstCamp.ID.Bytes)

				if bestID == worstID {
					continue
				}

				shiftAmount := s.cfg.AutoscaleShiftAmount
				worstLimit := worstCamp.BudgetLimit
				bestLimit := bestCamp.BudgetLimit

				newWorstLimit := worstLimit - shiftAmount
				newBestLimit := bestLimit + shiftAmount

				_, err = q.UpdateCampaignBudget(ctx, db.UpdateCampaignBudgetParams{
					ID:          worstCamp.ID,
					BudgetLimit: newWorstLimit,
				})
				if err != nil {
					return fmt.Errorf("failed to decrease budget for campaign %s: %w", worstID, err)
				}

				_, err = q.UpdateCampaignBudget(ctx, db.UpdateCampaignBudgetParams{
					ID:          bestCamp.ID,
					BudgetLimit: newBestLimit,
				})
				if err != nil {
					return fmt.Errorf("failed to increase budget for campaign %s: %w", bestID, err)
				}

				worstLimitStr := fmt.Sprintf("%.2f", float64(worstLimit)/1_000_000.0)
				newWorstLimitStr := fmt.Sprintf("%.2f", float64(newWorstLimit)/1_000_000.0)
				bestLimitStr := fmt.Sprintf("%.2f", float64(bestLimit)/1_000_000.0)
				newBestLimitStr := fmt.Sprintf("%.2f", float64(newBestLimit)/1_000_000.0)

				s.AuditLog(ctx, q, uuid.Nil, "AUTOSCALE_BUDGET_TRANSFER", "campaign", &worstID, map[string]any{
					"old_budget": worstLimitStr,
					"new_budget": newWorstLimitStr,
					"ctr":        worstCTR,
					"target":     bestID.String(),
				}, nil)

				s.AuditLog(ctx, q, uuid.Nil, "AUTOSCALE_BUDGET_TRANSFER", "campaign", &bestID, map[string]any{
					"old_budget": bestLimitStr,
					"new_budget": newBestLimitStr,
					"ctr":        bestCTR,
					"source":     worstID.String(),
				}, nil)

				worstPayload, _ := json.Marshal(CampaignPayload{
					CampaignID:  worstID.String(),
					BudgetLimit: newWorstLimit,
				})
				bestPayload, _ := json.Marshal(CampaignPayload{
					CampaignID:  bestID.String(),
					BudgetLimit: newBestLimit,
				})

				_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
					EventType: "CREATE_CAMPAIGN",
					Payload:   worstPayload,
				})
				if err != nil {
					return fmt.Errorf("failed to create outbox event for worst campaign: %w", err)
				}

				_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
					EventType: "CREATE_CAMPAIGN",
					Payload:   bestPayload,
				})
				if err != nil {
					return fmt.Errorf("failed to create outbox event for best campaign: %w", err)
				}

				slog.Info("autoscaled budgets by rule",
					"customer_id", custID,
					"decreased_campaign", worstID,
					"decreased_ctr", worstCTR,
					"increased_campaign", bestID,
					"increased_ctr", bestCTR,
					"shift_amount", shiftAmount,
				)
			}
		}

		return nil
	})
}
