package ads

import (
	"context"
	"fmt"
	"time"

	"espx/internal/domain"
	"espx/internal/metrics"
	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

// budgetKeyTTL keeps warmed budget keys alive across daily registry sync cycles.
const budgetKeyTTL = 24 * time.Hour

// RemainingBudgetMicro computes non-negative remaining budget from registry snapshot fields.
func RemainingBudgetMicro(c *domain.Campaign) int64 {
	if c == nil {
		return 0
	}
	rem := c.BudgetLimit - c.CurrentSpend
	if rem < 0 {
		return 0
	}
	return rem
}

// BudgetCacheWarmer seeds Redis budget keys before the filter hot path sees cache misses.
type BudgetCacheWarmer struct {
	rdbs    []redis.UniversalClient
	sharder Sharder
}

// NewBudgetCacheWarmer creates a shard-aware warmer for campaign budget keys.
func NewBudgetCacheWarmer(rdbs []redis.UniversalClient, sharder Sharder) *BudgetCacheWarmer {
	return &BudgetCacheWarmer{rdbs: rdbs, sharder: sharder}
}

// budgetWarmItem pairs a Redis key with the remaining budget to seed.
type budgetWarmItem struct {
	key string
	val int64
}

// Warm inserts missing budget keys on each shard without overwriting live counters.
func (w *BudgetCacheWarmer) Warm(ctx context.Context, campaigns []*domain.Campaign) (int, error) {
	if w == nil || len(w.rdbs) == 0 || len(campaigns) == 0 {
		return 0, nil
	}

	byShard := make([][]budgetWarmItem, len(w.rdbs))
	for _, camp := range campaigns {
		if camp == nil || camp.BudgetCampaignKey == "" {
			continue
		}
		shard := w.sharder.GetShard(camp.ID)
		if shard < 0 || shard >= len(w.rdbs) {
			continue
		}
		byShard[shard] = append(byShard[shard], budgetWarmItem{
			key: camp.BudgetCampaignKey,
			val: RemainingBudgetMicro(camp),
		})
	}

	warmed := 0
	for shard, items := range byShard {
		if len(items) == 0 {
			continue
		}
		pipe := w.rdbs[shard].Pipeline()
		cmds := make([]*redis.BoolCmd, len(items))
		for i, item := range items {
			cmds[i] = pipe.SetNX(ctx, item.key, item.val, budgetKeyTTL)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return warmed, fmt.Errorf("budget warm pipeline shard %d: %w", shard, err)
		}
		for _, cmd := range cmds {
			if cmd.Val() {
				warmed++
			}
		}
	}
	metrics.BudgetCacheWarmTotal.WithLabelValues("full").Add(float64(warmed))
	return warmed, nil
}

// WarmOne seeds one campaign budget key after an incremental registry update.
func (w *BudgetCacheWarmer) WarmOne(ctx context.Context, camp *domain.Campaign) (bool, error) {
	if w == nil || len(w.rdbs) == 0 || camp == nil || camp.BudgetCampaignKey == "" {
		return false, nil
	}
	shard := w.sharder.GetShard(camp.ID)
	if shard < 0 || shard >= len(w.rdbs) {
		return false, fmt.Errorf("invalid shard index %d for campaign %s", shard, camp.ID)
	}

	rdb := w.rdbs[shard]
	remaining := RemainingBudgetMicro(camp)

	warmed, err := rdb.SetNX(ctx, camp.BudgetCampaignKey, remaining, budgetKeyTTL).Result()
	if err != nil {
		return false, fmt.Errorf("budget warm one shard %d: %w", shard, err)
	}

	if warmed {
		metrics.BudgetCacheWarmTotal.WithLabelValues("incremental").Inc()
	}
	return warmed, nil
}

// WarmFromRegistry warms all active campaigns from the in-memory registry snapshot.
func (w *BudgetCacheWarmer) WarmFromRegistry(ctx context.Context, reg *CampaignRegistry) (int, error) {
	if reg == nil {
		return 0, nil
	}
	return w.Warm(ctx, reg.ActiveCampaigns())
}

// warmBudgetKeyNX inserts a budget key when Lua reports a cache miss.
func warmBudgetKeyNX(ctx context.Context, rdb redis.UniversalClient, key string, remaining int64) error {
	_, err := rdb.SetNX(ctx, key, remaining, budgetKeyTTL).Result()
	return err
}

// tryRecoverBudgetFromRegistry reloads budget from the registry before hitting Postgres.
func tryRecoverBudgetFromRegistry(
	ctx context.Context,
	rdb redis.UniversalClient,
	registry domain.CampaignRegistry,
	campaignID uuid.UUID,
	budgetKey string,
) (bool, error) {
	if registry == nil {
		return false, nil
	}
	camp, ok := registry.GetCampaign(campaignID)
	if !ok {
		return false, nil
	}
	if camp.BudgetLimit == 0 && camp.CurrentSpend == 0 {
		return false, nil
	}
	if err := warmBudgetKeyNX(ctx, rdb, budgetKey, RemainingBudgetMicro(camp)); err != nil {
		return false, err
	}
	metrics.BudgetCacheRegistryRecoverTotal.Inc()
	return true, nil
}
