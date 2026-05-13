package ads

import (
	"context"
	_ "embed"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/sharding"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	redis "github.com/redis/go-redis/v9"
)

//go:embed unified_filter.lua
var unifiedFilterLua string

// UnifiedFilter coordinates multi-stage event validation using Redis Lua scripts.
// It integrates rate limiting, deduplication, and budget tracking in a single atomic operation.
type UnifiedFilter struct {
	rdbs             []redis.UniversalClient
	sharder          sharding.Sharder
	script           *redis.Script
	registry         domain.CampaignRegistry
	repo             domain.CampaignRepository
	rateLimit        int
	rateLimitWindow  time.Duration
	dupTTL           time.Duration
	idempotencyTTL   time.Duration
	clickAmount      float64
	impressionAmount float64
	streamName       string
	maxStreamLen     int
}

// NewUnifiedFilter initializes the filter with sharded Redis clients and persistence repositories.
// Chosen to centralize validation logic and ensure atomic state transitions across shards.
func NewUnifiedFilter(
	rdbs []redis.UniversalClient,
	sharder sharding.Sharder,
	registry domain.CampaignRegistry,
	repo domain.CampaignRepository,
	rateLimit int,
	rateLimitWindow time.Duration,
	dupTTL time.Duration,
	idempotencyTTL time.Duration,
	clickAmount float64,
	impressionAmount float64,
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

// getRDB selects the appropriate Redis shard based on the campaign ID.
// Uses a consistent sharder to minimize key migration during cluster resizing.
func (f *UnifiedFilter) getRDB(campaignID uuid.UUID) redis.UniversalClient {
	if len(f.rdbs) <= 1 {
		return f.rdbs[0]
	}
	idx := f.sharder.GetShard(campaignID)
	return f.rdbs[idx%len(f.rdbs)]
}

// Check executes the validation script and handles budget cache misses via PostgreSQL.
// Implements an iterative retry loop to prevent recursion while ensuring ingestion availability.
func (f *UnifiedFilter) Check(ctx context.Context, evt *domain.Event) error {
	customerID, ok := f.registry.GetCustomerID(evt.CampaignID)
	if !ok {
		return ErrCampaignNotFound
	}

	if evt.ClickID == "" {
		id, _ := uuid.NewV7()
		evt.ClickID = id.String()
	}

	campaignIDStr := evt.CampaignID.String()
	customerIDStr := customerID.String()

	amount := f.clickAmount
	if evt.Type == "impression" {
		amount = f.impressionAmount
	}

	rdb := f.getRDB(evt.CampaignID)

	sb := builderPool.Get().(*strings.Builder)
	defer builderPool.Put(sb)

	sb.Reset()
	sb.WriteString("rl:ip:")
	sb.WriteString(evt.IP)
	rlKey := sb.String()

	sb.Reset()
	sb.WriteString("dup:")
	sb.WriteString(evt.Type)
	sb.WriteByte(':')
	sb.WriteString(evt.ClickID)
	dupKey := sb.String()

	sb.Reset()
	sb.WriteString("budget:campaign:")
	sb.WriteString(campaignIDStr)
	budgetSourceKey := sb.String()

	sb.Reset()
	sb.WriteString("idempotency:click:")
	sb.WriteString(evt.ClickID)
	idempotencyKey := sb.String()

	sb.Reset()
	sb.WriteString("budget:sync:campaign:")
	sb.WriteString(campaignIDStr)
	campaignSyncKey := sb.String()

	sb.Reset()
	sb.WriteString("budget:sync:customer:")
	sb.WriteString(customerIDStr)
	customerSyncKey := sb.String()

	dirtyCampaignsKey := "budget:dirty_campaigns"
	dirtyCustomersKey := "budget:dirty_customers"
	streamKey := f.streamName

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
			},
			int(f.rateLimitWindow.Seconds()),
			f.rateLimit,
			int(f.dupTTL.Seconds()),
			amount,
			int(f.idempotencyTTL.Seconds()),
			campaignIDStr,
			customerIDStr,
			f.maxStreamLen,
			evt.ClickID,
			evt.Type,
			evt.Payload,
			evt.IP,
			evt.UA,
		).Int64()

		if err != nil {
			return nil
		}

		if res == -1 {
			if i > 0 {
				return nil
			}

			camp, err := f.repo.GetByID(ctx, evt.CampaignID)
			if err != nil {
				return nil
			}

			remaining := camp.BudgetLimit - camp.CurrentSpend
			if remaining < 0 {
				remaining = 0
			}

			rdb.SetNX(ctx, budgetSourceKey, remaining, 24*time.Hour)
			continue
		}

		switch res {
		case 1:
			return ErrRateLimitExceeded
		case 2:
			return ErrDuplicateEvent
		case 3:
			return ErrBudgetExhausted
		default:
			return nil
		}
	}

	return nil
}
