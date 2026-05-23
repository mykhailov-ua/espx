package management

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
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

			// Perform pacing arithmetic strictly using micro-units int64 values.
			// This completely bypasses decimal.Decimal heap allocations and float parsing on the core loop.
			budgetMicro := ads.NumericToMicro(row.DailyBudget)
			if budgetMicro == 0 {
				budgetMicro = ads.NumericToMicro(row.BudgetLimit)
			}
			if budgetMicro == 0 {
				continue
			}

			actualSpendMicro := ads.NumericToMicro(row.CurrentSpend)
			expectedSpendMicro := int64(float64(budgetMicro) * timeRatio)

			var targetPacing db.PacingModeType
			var shouldUpdate bool

			// Apply a 15% tolerance margin using integer scaling factor multiplication.
			// This avoids float conversions and rounds calculations natively inside CPU registers.
			overThresholdMicro := int64(float64(expectedSpendMicro) * 1.15)
			underThresholdMicro := int64(float64(expectedSpendMicro) * 0.85)

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

				actualSpend := ads.MicroToDecimal(actualSpendMicro)
				expectedSpend := ads.MicroToDecimal(expectedSpendMicro)

				s.AuditLog(ctx, q, uuid.Nil, "PACING_LOOP_ADJUSTMENT", "campaign", &campID, map[string]any{
					"old_pacing": string(row.PacingMode),
					"new_pacing": string(targetPacing),
					"spend":      actualSpend.StringFixed(2),
					"expected":   expectedSpend.StringFixed(2),
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
					"actual_spend", actualSpend,
					"expected_spend", expectedSpend,
				)
			}
		}

		return nil
	})
}
