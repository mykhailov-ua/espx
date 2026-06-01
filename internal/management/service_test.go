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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagementService_CancelCampaign(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)

	rdb, cleanupRedis := database.SetupTestRedis(t)

	cfg := &config.Config{}
	cfg.Management.CancellationFeePercent = 10.0
	cfg.Lifecycle.WaitTimeoutMs = 500
	cfg.CampaignUpdateChannel = "test:campaign:updates"

	sharder := ads.NewJumpHashSharder(1)
	svc := NewService(pool, []redis.UniversalClient{rdb}, sharder, cfg)

	t.Cleanup(func() {
		svc.Close()
		cleanupRedis()
		cleanupDB()
	})

	ctx := context.Background()
	customerID := uuid.New()
	err := svc.CreateCustomer(ctx, customerID, "Test Advertiser", 1_000_000_000, "USD")
	require.NoError(t, err)

	budget := int64(500_000_000)
	campaignID, err := svc.CreateCampaign(ctx, customerID, nil, "Test Campaign", budget, db.PacingModeTypeASAP, 0, "UTC", 0, 0, nil, "idemp-1")
	require.NoError(t, err)

	_, _ = pool.Exec(ctx, "UPDATE campaigns SET current_spend = $1 WHERE id = $2", int64(200_000_000), campaignID)

	err = svc.CancelCampaign(ctx, campaignID, "db.User request")
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		var balance string
		err := pool.QueryRow(ctx, "SELECT balance::TEXT FROM customers WHERE id = $1", customerID).Scan(&balance)
		if err != nil || balance != "770000000" {
			return false
		}
		var status string
		err = pool.QueryRow(ctx, "SELECT status FROM campaigns WHERE id = $1", campaignID).Scan(&status)
		return err == nil && status == "DELETED"
	}, 2*time.Second, 20*time.Millisecond)
}

func TestManagementService_Idempotency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)

	rdb, cleanupRedis := database.SetupTestRedis(t)

	cfg := &config.Config{}
	cfg.Lifecycle.WaitTimeoutMs = 500

	svc := NewService(pool, []redis.UniversalClient{rdb}, ads.NewJumpHashSharder(1), cfg)

	t.Cleanup(func() {
		svc.Close()
		cleanupRedis()
		cleanupDB()
	})
	ctx := context.Background()
	customerID := uuid.New()
	_ = svc.CreateCustomer(ctx, customerID, "Idem Tester", 1_000_000_000, "USD")

	amount := int64(100_000_000)
	hash := "topup-1"

	err := svc.TopUpBalance(ctx, customerID, amount, hash)
	require.NoError(t, err)

	err = svc.TopUpBalance(ctx, customerID, amount, hash)
	require.NoError(t, err)

	var balance string
	_ = pool.QueryRow(ctx, "SELECT balance::TEXT FROM customers WHERE id = $1", customerID).Scan(&balance)
	assert.Equal(t, "1100000000", balance)
}
