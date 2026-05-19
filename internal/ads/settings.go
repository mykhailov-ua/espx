package ads

import (
// "github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
)

import (
	"context"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

// DynamicConfig represents settings that can be updated at runtime via the Management API.
type DynamicConfig struct {
	Version          int64           `json:"version"`
	RateLimitPerMin  int             `json:"rate_limit_per_min"`
	RateLimitWindow  int             `json:"rate_limit_window_ms"`
	ClickAmount      decimal.Decimal `json:"click_amount"`
	ImpressionAmount decimal.Decimal `json:"impression_amount"`
}

// SettingsWatcher periodically checks Redis for configuration updates and performs atomic swaps of the local state.
type SettingsWatcher struct {
	rdb            redis.UniversalClient
	currentVersion int64
	snapshot       atomic.Value // holds *DynamicConfig
}

func NewSettingsWatcher(rdb redis.UniversalClient, initial *config.Config) *SettingsWatcher {
	sw := &SettingsWatcher{
		rdb: rdb,
	}

	// Bootstrap from static config
	sw.snapshot.Store(&DynamicConfig{
		Version:          0,
		RateLimitPerMin:  initial.RateLimitPerMin,
		RateLimitWindow:  initial.RateLimitWindowMs,
		ClickAmount:      initial.ClickAmount,
		ImpressionAmount: initial.ImpressionAmount,
	})

	return sw
}

// Get returns the current consistent snapshot of dynamic settings.
func (sw *SettingsWatcher) Get() *DynamicConfig {
	return sw.snapshot.Load().(*DynamicConfig)
}

// Start initiates the polling loop to synchronize settings with Redis.
func (sw *SettingsWatcher) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sw.sync(ctx)
		}
	}
}

func (sw *SettingsWatcher) sync(ctx context.Context) {
	v, err := sw.rdb.Get(ctx, "config:version").Int64()
	if err != nil {
		if err != redis.Nil {
			slog.Error("failed to check config version", "error", err)
		}
		return
	}

	if v <= sw.currentVersion {
		return
	}

	data, err := sw.rdb.HGetAll(ctx, "config:values").Result()
	if err != nil {
		slog.Error("failed to fetch config values", "error", err)
		return
	}

	newCfg := sw.parseConfig(v, data)
	sw.snapshot.Store(newCfg)
	sw.currentVersion = v

	slog.Info("dynamic settings updated", "version", v)
}

func (sw *SettingsWatcher) parseConfig(version int64, data map[string]string) *DynamicConfig {
	current := sw.Get()
	next := *current
	next.Version = version

	updateInt(&next.RateLimitPerMin, data["rate_limit_per_min"])
	updateInt(&next.RateLimitWindow, data["rate_limit_window_ms"])
	updateDecimal(&next.ClickAmount, data["click_amount"])
	updateDecimal(&next.ImpressionAmount, data["impression_amount"])

	return &next
}

func updateInt(target *int, val string) {
	if val == "" {
		return
	}
	if i, err := strconv.Atoi(val); err == nil {
		*target = i
	}
}

func updateDecimal(target *decimal.Decimal, val string) {
	if val == "" {
		return
	}
	if d, err := decimal.NewFromString(val); err == nil {
		*target = d
	}
}
