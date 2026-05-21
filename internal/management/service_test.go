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

func TestManagementService_CancelCampaign(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{}
	cfg.Management.CancellationFeePercent = 10.0
	cfg.Lifecycle.WaitTimeoutMs = 10
	cfg.CampaignUpdateChannel = "test:campaign:updates"

	sharder := ads.NewJumpHashSharder(1)
	svc := NewService(pool, []redis.UniversalClient{rdb}, sharder, cfg)
	defer svc.Close()

	ctx := context.Background()
	customerID := uuid.New()
	err := svc.CreateCustomer(ctx, customerID, "Test Advertiser", decimal.NewFromInt(1000), "USD")
	require.NoError(t, err)

	budget := decimal.NewFromInt(500)
	campaignID, err := svc.CreateCampaign(ctx, customerID, nil, "Test Campaign", budget, db.PacingModeTypeASAP, decimal.Zero, "UTC", 0, 0, nil, "idemp-1")
	require.NoError(t, err)

	_, _ = pool.Exec(ctx, "UPDATE campaigns SET current_spend = $1 WHERE id = $2", ads.ToNumeric(decimal.NewFromInt(200)), campaignID)

	err = svc.CancelCampaign(ctx, campaignID, "db.User request")
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		var balance string
		err := pool.QueryRow(ctx, "SELECT balance::TEXT FROM customers WHERE id = $1", customerID).Scan(&balance)
		if err != nil || balance != "770.00" {
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
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	svc := NewService(pool, []redis.UniversalClient{rdb}, ads.NewJumpHashSharder(1), &config.Config{})
	defer svc.Close()
	ctx := context.Background()
	customerID := uuid.New()
	_ = svc.CreateCustomer(ctx, customerID, "Idem Tester", decimal.NewFromInt(1000), "USD")

	amount := decimal.NewFromInt(100)
	hash := "topup-1"

	err := svc.TopUpBalance(ctx, customerID, amount, hash)
	require.NoError(t, err)

	err = svc.TopUpBalance(ctx, customerID, amount, hash)
	require.NoError(t, err)

	var balance string
	_ = pool.QueryRow(ctx, "SELECT balance::TEXT FROM customers WHERE id = $1", customerID).Scan(&balance)
	assert.Equal(t, "1100.00", balance)
}
