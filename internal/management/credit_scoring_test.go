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
	"github.com/shopspring/decimal"
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
	err := svc.CreateCustomer(ctx, customerID, "Credit Customer", decimal.Zero, "USD")
	require.NoError(t, err)

	// 2. Validate that creating a campaign with budget $100 fails due to insufficient balance.
	_, err = svc.CreateCampaign(ctx, customerID, nil, "Poor Campaign", decimal.NewFromInt(100), db.PacingModeTypeASAP, decimal.Zero, "UTC", 0, 0, nil, "poor-idem")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient balance")

	// 3. Simulate account tenure of 35 days by updating created_at field.
	_, err = pool.Exec(ctx, "UPDATE customers SET created_at = NOW() - INTERVAL '35 days' WHERE id = $1", ads.ToUUID(customerID))
	require.NoError(t, err)

	// 4. Populate ledger with top-ups totalling $1000 inside the 30-day window.
	err = svc.TopUpBalance(ctx, customerID, decimal.NewFromInt(1000), "topup-scoring-1")
	require.NoError(t, err)

	// Verify current cash balance is $1000.
	var balance string
	err = pool.QueryRow(ctx, "SELECT balance::TEXT FROM customers WHERE id = $1", customerID).Scan(&balance)
	require.NoError(t, err)
	assert.Equal(t, "1000.00", balance)

	// 5. Trigger the CreditScoringWorker manual evaluation.
	worker := NewCreditScoringWorker(svc)
	err = worker.EvaluateAll(ctx)
	require.NoError(t, err)

	// Verify allowed overdraft is computed as 30% of $1000 = $300.
	var allowedOverdraft string
	err = pool.QueryRow(ctx, "SELECT allowed_overdraft::TEXT FROM customers WHERE id = $1", customerID).Scan(&allowedOverdraft)
	require.NoError(t, err)
	assert.Equal(t, "300.00", allowedOverdraft)

	// 6. Withdraw the cash balance to simulate deficit or spending.
	_, err = pool.Exec(ctx, "UPDATE customers SET balance = 0.00 WHERE id = $1", ads.ToUUID(customerID))
	require.NoError(t, err)

	// 7. Verify a campaign with budget $200 can be successfully created within the $300 overdraft margin.
	campaignID, err := svc.CreateCampaign(ctx, customerID, nil, "Credit Campaign", decimal.NewFromInt(200), db.PacingModeTypeASAP, decimal.Zero, "UTC", 0, 0, nil, "credit-idem")
	require.NoError(t, err)
	assert.NotEqual(t, uuid.Nil, campaignID)

	// Verify cash balance went negative to -$200.
	err = pool.QueryRow(ctx, "SELECT balance::TEXT FROM customers WHERE id = $1", customerID).Scan(&balance)
	require.NoError(t, err)
	assert.Equal(t, "-200.00", balance)

	// 8. Verify campaign creation fails if it exceeds the remaining overdraft budget.
	_, err = svc.CreateCampaign(ctx, customerID, nil, "Excessive Campaign", decimal.NewFromInt(150), db.PacingModeTypeASAP, decimal.Zero, "UTC", 0, 0, nil, "excessive-idem")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient balance")

	// 9. Decrease allowed overdraft to $100, which triggers campaign suspension and budget release since availableLimit would go negative (-$200 + $100 = -$100).
	err = svc.UpdateOverdraft(ctx, customerID, decimal.NewFromInt(100), decimal.NewFromInt(300))
	require.NoError(t, err)

	// Verify that the campaign status is now PAUSED in the DB.
	var campaignStatus string
	err = pool.QueryRow(ctx, "SELECT status::TEXT FROM campaigns WHERE id = $1", ads.ToUUID(campaignID)).Scan(&campaignStatus)
	require.NoError(t, err)
	assert.Equal(t, "PAUSED", campaignStatus)

	// Verify customer balance went back to $0.00 since the remaining $200.00 budget was released.
	err = pool.QueryRow(ctx, "SELECT balance::TEXT FROM customers WHERE id = $1", customerID).Scan(&balance)
	require.NoError(t, err)
	assert.Equal(t, "0.00", balance)

	// Verify overdraft is now $100.00 in the DB.
	err = pool.QueryRow(ctx, "SELECT allowed_overdraft::TEXT FROM customers WHERE id = $1", customerID).Scan(&allowedOverdraft)
	require.NoError(t, err)
	assert.Equal(t, "100.00", allowedOverdraft)

	// Verify a CANCEL_CAMPAIGN outbox event was generated to drop the campaign from Redis and tracker memory.
	var outboxCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE event_type = 'CANCEL_CAMPAIGN'").Scan(&outboxCount)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, outboxCount, 1)
}
