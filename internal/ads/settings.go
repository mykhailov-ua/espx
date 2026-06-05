// Package ads provides SettingsWatcher, a live-reloadable configuration layer for
// runtime-adjustable parameters (rate limit, event charge amounts, emergency breaker).
// The current config is stored in an atomic.Value snapshot; callers read it via Get
// without any lock contention. Updates are driven by a Redis config:version counter:
// the watcher polls at interval; if the stored version is higher than its local copy
// it fetches config:values via HGetAll, parses the new config, and atomically swaps
// the snapshot. Version-gated polling avoids redundant HGetAll calls when the config
// has not changed since the last tick.
//
// The initial config is seeded from the static Config struct at construction time;
// subsequent versions are layered on top (un-set fields retain the previous value).
package ads

import (
	"context"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/redis/go-redis/v9"
)

// DynamicConfig is the live-reloadable subset of Config. Fields are expressed in
// their natural units (not micro-units) except ClickAmount and ImpressionAmount
// which are stored as int64 micro-unit values consistent with the rest of the
// budget subsystem.
type DynamicConfig struct {
	Version          int64 `json:"version"`
	RateLimitPerMin  int   `json:"rate_limit_per_min"`
	RateLimitWindow  int   `json:"rate_limit_window_ms"`
	ClickAmount      int64 `json:"click_amount"`
	ImpressionAmount int64 `json:"impression_amount"`
	EmergencyBreaker bool  `json:"emergency_breaker"`
}

// SettingsWatcher polls Redis for config version changes and updates the
// atomic DynamicConfig snapshot. The currentVersion field is read and written
// with atomic operations even though it is also accessed under no lock, because
// the snapshot swap and the version store are not required to be atomic with each
// other - a reader may briefly see a version N+1 snapshot with version N stored,
// which is harmless.
type SettingsWatcher struct {
	rdb            redis.UniversalClient
	currentVersion int64
	snapshot       atomic.Value
}

func NewSettingsWatcher(rdb redis.UniversalClient, initial *config.Config) *SettingsWatcher {
	sw := &SettingsWatcher{
		rdb: rdb,
	}

	sw.snapshot.Store(&DynamicConfig{
		Version:          0,
		RateLimitPerMin:  initial.RateLimitPerMin,
		RateLimitWindow:  initial.RateLimitWindowMs,
		ClickAmount:      initial.ClickAmount,
		ImpressionAmount: initial.ImpressionAmount,
		EmergencyBreaker: false,
	})

	return sw
}

// Get returns the current DynamicConfig snapshot. Callers must not mutate the
// returned pointer; doing so would create a data race with the sync goroutine.
func (sw *SettingsWatcher) Get() *DynamicConfig {
	return sw.snapshot.Load().(*DynamicConfig)
}

// Start runs the version-poll loop in the calling goroutine. It is intended to be
// launched in a dedicated goroutine; it exits when ctx is cancelled.
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

	if v <= atomic.LoadInt64(&sw.currentVersion) {
		return
	}

	data, err := sw.rdb.HGetAll(ctx, "config:values").Result()
	if err != nil {
		slog.Error("failed to fetch config values", "error", err)
		return
	}

	newCfg := sw.parseConfig(v, data)
	sw.snapshot.Store(newCfg)
	atomic.StoreInt64(&sw.currentVersion, v)

	slog.Info("dynamic settings updated", "version", v)
}

func (sw *SettingsWatcher) parseConfig(version int64, data map[string]string) *DynamicConfig {
	current := sw.Get()
	next := *current
	next.Version = version

	updateInt(&next.RateLimitPerMin, data["rate_limit_per_min"])
	updateInt(&next.RateLimitWindow, data["rate_limit_window_ms"])
	updateMicro(&next.ClickAmount, data["click_amount"])
	updateMicro(&next.ImpressionAmount, data["impression_amount"])
	updateBool(&next.EmergencyBreaker, data["emergency_breaker"])

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

func updateMicro(target *int64, val string) {
	if val == "" {
		return
	}
	if f, err := strconv.ParseFloat(val, 64); err == nil {
		*target = int64(f * 1_000_000)
	}
}

func updateBool(target *bool, val string) {
	if val == "" {
		return
	}
	if b, err := strconv.ParseBool(val); err == nil {
		*target = b
	}
}
