package management

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (s *Service) ClosedLoopPacingController(ctx context.Context, syncWorkers []*ads.SyncWorker) error {
	for _, sw := range syncWorkers {
		sw.SyncAll(ctx)
	}

	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		rows, err := q.GetAllActiveCampaignsWithStats(ctx)
		if err != nil {
			return fmt.Errorf("failed to fetch active campaigns for pacing: %w", err)
		}

		now := time.Now()
		for _, row := range rows {
			var loc *time.Location
			if cached, found := s.locCache.Load(row.Timezone); found {
				loc = cached.(*time.Location)
			} else {
				var err error
				loc, err = time.LoadLocation(row.Timezone)
				if err != nil {
					loc = time.UTC
				}
				s.locCache.Store(row.Timezone, loc)
			}
			localNow := now.In(loc)

			startOfDay := time.Date(localNow.Year(), localNow.Month(), localNow.Day(), 0, 0, 0, 0, loc)
			secondsElapsed := localNow.Sub(startOfDay).Seconds()
			if secondsElapsed < 0 {
				secondsElapsed = 0
			}
			const totalSeconds = 86400.0
			timeRatio := secondsElapsed / totalSeconds
			if timeRatio > 1.0 {
				timeRatio = 1.0
			}

			budgetMicro := row.DailyBudget
			if budgetMicro == 0 {
				budgetMicro = row.BudgetLimit
			}
			if budgetMicro == 0 {
				continue
			}

			actualSpendMicro := row.CurrentSpend
			expectedSpendMicro := int64(float64(budgetMicro) * timeRatio)

			var targetPacing db.PacingModeType
			var shouldUpdate bool

			overThresholdMicro := int64(float64(expectedSpendMicro) * (1.0 + s.cfg.PacingToleranceMargin))
			underThresholdMicro := int64(float64(expectedSpendMicro) * (1.0 - s.cfg.PacingToleranceMargin))

			if row.PacingMode == db.PacingModeTypeASAP && actualSpendMicro > overThresholdMicro {
				targetPacing = db.PacingModeTypeEVEN
				shouldUpdate = true
			} else if row.PacingMode == db.PacingModeTypeEVEN && actualSpendMicro < underThresholdMicro {
				targetPacing = db.PacingModeTypeASAP
				shouldUpdate = true
			}

			if shouldUpdate {
				campID := uuid.UUID(row.ID.Bytes)
				_, err = q.UpdateCampaignPacing(ctx, db.UpdateCampaignPacingParams{
					ID:         row.ID,
					PacingMode: targetPacing,
				})
				if err != nil {
					return fmt.Errorf("failed to update pacing mode: %w", err)
				}

				actualSpendStr := fmt.Sprintf("%.2f", float64(actualSpendMicro)/1_000_000.0)
				expectedSpendStr := fmt.Sprintf("%.2f", float64(expectedSpendMicro)/1_000_000.0)

				s.AuditLog(ctx, q, uuid.Nil, "PACING_LOOP_ADJUSTMENT", "campaign", &campID, map[string]any{
					"old_pacing": string(row.PacingMode),
					"new_pacing": string(targetPacing),
					"spend":      actualSpendStr,
					"expected":   expectedSpendStr,
				}, nil)

				payloadBytes, err := json.Marshal(map[string]any{
					"campaign_id": campID.String(),
					"pacing_mode": string(targetPacing),
				})
				if err != nil {
					return fmt.Errorf("failed to marshal pacing outbox payload: %w", err)
				}

				_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
					EventType: "UPDATE_CAMPAIGN_PACING",
					Payload:   payloadBytes,
				})
				if err != nil {
					return fmt.Errorf("failed to create outbox event for pacing: %w", err)
				}

				slog.Info("closed-loop pacing controller adjusted pacing",
					"campaign_id", campID,
					"old_pacing", row.PacingMode,
					"new_pacing", targetPacing,
					"actual_spend", actualSpendStr,
					"expected_spend", expectedSpendStr,
				)
			}
		}

		return nil
	})
}
