package management

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/database"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegistryWatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queries := db.New(pool)
	registry := ads.NewRegistry(queries)

	channel := "test:campaign:updates"
	registry.StartWatch(ctx, rdb, channel)

	cfg := &config.Config{
		CampaignUpdateChannel: channel,
	}
	cfg.Lifecycle.WaitTimeoutMs = 1
	sharder := ads.NewJumpHashSharder(1)
	svc := NewService(pool, []redis.UniversalClient{rdb}, sharder, cfg)
	defer svc.Close()

	customerID := uuid.New()
	_ = svc.CreateCustomer(ctx, customerID, "Sync db.User", decimal.NewFromInt(1000), "USD")

	campaignID, err := svc.CreateCampaign(ctx, customerID, nil, "Sync Camp", decimal.NewFromInt(100), db.PacingModeTypeASAP, decimal.Zero, "UTC", 0, 0, nil, "idemp-sync")
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		return registry.Exists(campaignID)
	}, 2*time.Second, 100*time.Millisecond)

	err = svc.CancelCampaign(ctx, campaignID, "Test Sync")
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		return !registry.Exists(campaignID)
	}, 2*time.Second, 100*time.Millisecond)
}
