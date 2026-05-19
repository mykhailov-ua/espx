package ads

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/mykhailov-ua/ad-event-processor/internal/metrics"
	redis "github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

//go:embed unified_filter.lua
var unifiedFilterLua string

type UnifiedFilter struct {
	rdbs             []redis.UniversalClient
	sharder          Sharder
	script           *redis.Script
	registry         domain.CampaignRegistry
	repo             domain.CampaignRepository
	rateLimit        int
	rateLimitWindow  time.Duration
	dupTTL           time.Duration
	idempotencyTTL   time.Duration
	clickAmount      decimal.Decimal
	impressionAmount decimal.Decimal
	streamName       string
	maxStreamLen     int
}

func NewUnifiedFilter(
	rdbs []redis.UniversalClient,
	sharder Sharder,
	registry domain.CampaignRegistry,
	repo domain.CampaignRepository,
	rateLimit int,
	rateLimitWindow time.Duration,
	dupTTL time.Duration,
	idempotencyTTL time.Duration,
	clickAmount decimal.Decimal,
	impressionAmount decimal.Decimal,
	streamName string,
	maxStreamLen int,
) *UnifiedFilter {
	return &UnifiedFilter{
		rdbs:             rdbs,
		sharder:          sharder,
		script:           redis.NewScript(unifiedFilterLua),
		registry:         registry,
		repo:             repo,
		rateLimit:        rateLimit,
		rateLimitWindow:  rateLimitWindow,
		dupTTL:           dupTTL,
		idempotencyTTL:   idempotencyTTL,
		clickAmount:      clickAmount,
		impressionAmount: impressionAmount,
		streamName:       streamName,
		maxStreamLen:     maxStreamLen,
	}
}

func (f *UnifiedFilter) getRDB(campaignID uuid.UUID) redis.UniversalClient {
	if len(f.rdbs) <= 1 {
		return f.rdbs[0]
	}
	idx := f.sharder.GetShard(campaignID)
	return f.rdbs[idx%len(f.rdbs)]
}

func (f *UnifiedFilter) Check(ctx context.Context, evt *domain.Event) error {
	campInfo, ok := f.registry.GetCampaign(evt.CampaignID)
	if !ok {
		return ErrCampaignNotFound
	}

	if evt.ClickID == "" {
		id, _ := uuid.NewV7()
		evt.ClickID = id.String()
	}

	campaignIDStr := evt.CampaignID.String()
	customerIDStr := campInfo.CustomerID.String()

	amount := f.clickAmount
	if evt.Type == "impression" {
		amount = f.impressionAmount
	}

	rdb := f.getRDB(evt.CampaignID)

	rlKey := "rl:ip:" + evt.IP
	dupKey := "dup:" + evt.Type + ":" + evt.ClickID
	budgetSourceKey := "budget:campaign:" + campaignIDStr
	idempotencyKey := "idempotency:click:" + evt.ClickID
	campaignSyncKey := "budget:sync:campaign:" + campaignIDStr
	customerSyncKey := "budget:sync:customer:" + customerIDStr

	dirtyCampaignsKey := "budget:dirty_campaigns"
	dirtyCustomersKey := "budget:dirty_customers"
	streamKey := f.streamName

	now := time.Now().In(campInfo.Location)
	currentDate := now.Format("20060102")
	currentHour := now.Hour() + 1 // 1-24

	dailySpendKey := "budget:daily_spent:campaign:" + campaignIDStr + ":" + currentDate

	var fcapKey string
	if evt.UserID != "" {
		fcapKey = "fcap:c:" + campaignIDStr + ":u:" + evt.UserID
	} else {
		fcapKey = "fcap:ignored"
	}

	isEven := 0
	if campInfo.PacingMode == domain.PacingModeEven {
		isEven = 1
	}

	for i := 0; i < 2; i++ {
		res, err := f.script.Run(ctx, rdb,
			[]string{
				rlKey,
				dupKey,
				budgetSourceKey,
				idempotencyKey,
				campaignSyncKey,
				customerSyncKey,
				dirtyCampaignsKey,
				dirtyCustomersKey,
				streamKey,
				dailySpendKey,
				fcapKey,
			},
			int(f.rateLimitWindow.Seconds()),
			f.rateLimit,
			int(f.dupTTL.Seconds()),
			amount.InexactFloat64(),
			int(f.idempotencyTTL.Seconds()),
			campaignIDStr,
			customerIDStr,
			f.maxStreamLen,
			evt.ClickID,
			evt.Type,
			evt.Payload,
			evt.IP,
			evt.UA,
			isEven,
			campInfo.DailyBudget.InexactFloat64(),
			currentHour,
			evt.UserID,
			campInfo.FreqLimit,
			campInfo.FreqWindow,
		).Int64()

		if err != nil {
			return err
		}

		if res == -1 {
			if i > 0 {
				return fmt.Errorf("budget cache miss on retry: %w", ErrBudgetExhausted)
			}

			camp, err := f.repo.GetByID(ctx, evt.CampaignID)
			if err != nil {
				return fmt.Errorf("failed to load campaign from db: %w", err)
			}

			remaining := camp.BudgetLimit.Sub(camp.CurrentSpend)
			if remaining.IsNegative() {
				remaining = decimal.Zero
			}

			rdb.SetNX(ctx, budgetSourceKey, remaining.InexactFloat64(), 24*time.Hour)
			continue
		}

		switch res {
		case 1:
			return ErrRateLimitExceeded
		case 2:
			return ErrDuplicateEvent
		case 3:
			return ErrBudgetExhausted
		case 4:
			return ErrPacingExhausted
		case 5:
			return ErrFreqLimitExceeded
		default:
			metrics.EventsProcessed.Inc()
			return nil
		}
	}

	return nil
}
