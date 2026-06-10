package management

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/internal/config"
	"espx/internal/database"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
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
	registry.SetReplicaPath(filepath.Join(t.TempDir(), "campaigns_replica.json"))

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
	_ = svc.CreateCustomer(ctx, customerID, "Sync db.User", 1_000_000_000, "USD")

	campaignID, err := svc.CreateCampaign(ctx, customerID, nil, "Sync Camp", 100_000_000, db.PacingModeTypeASAP, 0, "UTC", 0, 0, nil, "idemp-sync")
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
