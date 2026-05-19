package ads

import (
	"context"
	"testing"
	"time"

	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/database"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSettingsWatcher(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &config.Config{
		RateLimitPerMin:  100,
		ClickAmount:      decimal.NewFromFloat(0.10),
		ImpressionAmount: decimal.NewFromFloat(0.01),
	}

	sw := NewSettingsWatcher(rdb, cfg)

	// Initial state
	assert.Equal(t, 100, sw.Get().RateLimitPerMin)
	assert.True(t, sw.Get().ClickAmount.Equal(decimal.NewFromFloat(0.10)))

	go sw.Start(ctx, 100*time.Millisecond)

	// Update settings in Redis
	err := rdb.HSet(ctx, "config:values", map[string]interface{}{
		"rate_limit_per_min": "200",
		"click_amount":       "0.25",
	}).Err()
	require.NoError(t, err)

	err = rdb.Incr(ctx, "config:version").Err()
	require.NoError(t, err)

	// Wait for sync
	assert.Eventually(t, func() bool {
		return sw.Get().RateLimitPerMin == 200 && sw.Get().ClickAmount.Equal(decimal.NewFromFloat(0.25))
	}, 2*time.Second, 200*time.Millisecond)

	assert.Equal(t, int64(1), sw.Get().Version)
}
