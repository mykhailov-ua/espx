package management

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/database"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreditScoringAndOverdraft verifies the operational cycle of the dynamic credit scoring worker.
// It sets up database records, simulates account age and top-ups, triggers scoring calculations, and confirms overdraft limit enforcement.
func TestCreditScoringAndOverdraft(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		CampaignUpdateChannel: "test:credit-updates",
	}

	sharder := ads.NewJumpHashSharder(1)
	svc := NewService(pool, []redis.UniversalClient{rdb}, sharder, cfg)
	defer svc.Close()

	ctx := context.Background()
	customerID := uuid.New()

	// 1. Create a customer with 0 initial balance.
	err := svc.CreateCustomer(ctx, customerID, "Credit Customer", 0, "USD")
	require.NoError(t, err)

	// 2. Validate that creating a campaign with budget $100 fails due to insufficient balance.
	_, err = svc.CreateCampaign(ctx, customerID, nil, "Poor Campaign", 100_000_000, db.PacingModeTypeASAP, 0, "UTC", 0, 0, nil, "poor-idem")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient balance")

	// 3. Simulate account tenure of 35 days by updating created_at field.
	_, err = pool.Exec(ctx, "UPDATE customers SET created_at = NOW() - INTERVAL '35 days' WHERE id = $1", ads.ToUUID(customerID))
	require.NoError(t, err)

	// 4. Populate ledger with top-ups totalling $1000 inside the 30-day window.
	err = svc.TopUpBalance(ctx, customerID, 1_000_000_000, "topup-scoring-1")
	require.NoError(t, err)

	// Verify current cash balance is $1000.
	var balance int64
	err = pool.QueryRow(ctx, "SELECT balance FROM customers WHERE id = $1", customerID).Scan(&balance)
	require.NoError(t, err)
	assert.Equal(t, int64(1_000_000_000), balance)

	// 5. Trigger the CreditScoringWorker manual evaluation.
	worker := NewCreditScoringWorker(svc)
	err = worker.EvaluateAll(ctx)
	require.NoError(t, err)

	// Verify allowed overdraft is computed as 30% of $1000 = $300.
	var allowedOverdraft int64
	err = pool.QueryRow(ctx, "SELECT allowed_overdraft FROM customers WHERE id = $1", customerID).Scan(&allowedOverdraft)
	require.NoError(t, err)
	assert.Equal(t, int64(300_000_000), allowedOverdraft)

	// 6. Withdraw the cash balance to simulate deficit or spending.
	_, err = pool.Exec(ctx, "UPDATE customers SET balance = 0 WHERE id = $1", ads.ToUUID(customerID))
	require.NoError(t, err)

	// 7. Verify a campaign with budget $200 can be successfully created within the $300 overdraft margin.
	campaignID, err := svc.CreateCampaign(ctx, customerID, nil, "Credit Campaign", 200_000_000, db.PacingModeTypeASAP, 0, "UTC", 0, 0, nil, "credit-idem")
	require.NoError(t, err)
	assert.NotEqual(t, uuid.Nil, campaignID)

	// Verify cash balance went negative to -$200.
	err = pool.QueryRow(ctx, "SELECT balance FROM customers WHERE id = $1", customerID).Scan(&balance)
	require.NoError(t, err)
	assert.Equal(t, int64(-200_000_000), balance)

	// 8. Verify campaign creation fails if it exceeds the remaining overdraft budget.
	_, err = svc.CreateCampaign(ctx, customerID, nil, "Excessive Campaign", 150_000_000, db.PacingModeTypeASAP, 0, "UTC", 0, 0, nil, "excessive-idem")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient balance")

	// 9. Decrease allowed overdraft to $100, which triggers campaign suspension and budget release since availableLimit would go negative (-$200 + $100 = -$100).
	err = svc.UpdateOverdraft(ctx, customerID, 100_000_000, 300_000_000)
	require.NoError(t, err)

	// Verify that the campaign status is now PAUSED in the DB.
	var campaignStatus string
	err = pool.QueryRow(ctx, "SELECT status::TEXT FROM campaigns WHERE id = $1", ads.ToUUID(campaignID)).Scan(&campaignStatus)
	require.NoError(t, err)
	assert.Equal(t, "PAUSED", campaignStatus)

	// Verify customer balance went back to $0.00 since the remaining $200.00 budget was released.
	err = pool.QueryRow(ctx, "SELECT balance FROM customers WHERE id = $1", customerID).Scan(&balance)
	require.NoError(t, err)
	assert.Equal(t, int64(0), balance)

	// Verify overdraft is now $100.00 in the DB.
	err = pool.QueryRow(ctx, "SELECT allowed_overdraft FROM customers WHERE id = $1", customerID).Scan(&allowedOverdraft)
	require.NoError(t, err)
	assert.Equal(t, int64(100_000_000), allowedOverdraft)

	// Verify a CANCEL_CAMPAIGN outbox event was generated to drop the campaign from Redis and tracker memory.
	var outboxCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE event_type = 'CANCEL_CAMPAIGN'").Scan(&outboxCount)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, outboxCount, 1)
}
