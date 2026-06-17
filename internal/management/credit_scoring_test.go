package management

import (
	"context"
	"testing"

	"espx/internal/ads"
	"espx/internal/config"
	"espx/internal/database"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreditScoringAndOverdraft guards credit scoring, overdraft limits, and campaign pause on overdraft shrink.
func TestCreditScoringAndOverdraft(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)

	rdb, cleanupRedis := database.SetupTestRedis(t)

	cfg := &config.Config{
		CampaignUpdateChannel:       "test:credit-updates",
		CreditScoringMinAgeDays:     7.0,
		CreditScoringMatureAgeDays:  30.0,
		CreditScoringMidTierPercent: 15,
		CreditScoringMaturePercent:  30,
		CreditScoringMaxCap:         10_000_000_000,
	}
	cfg.Lifecycle.WaitTimeoutMs = 500

	sharder := ads.NewJumpHashSharder(1)
	svc := NewService(pool, []redis.UniversalClient{rdb}, sharder, cfg)

	t.Cleanup(func() {
		svc.Close()
		cleanupRedis()
		cleanupDB()
	})

	ctx := context.Background()
	customerID := uuid.New()

	err := svc.CreateCustomer(ctx, customerID, "Credit Customer", 0, "USD")
	require.NoError(t, err)

	spec := testCampaignSpec(customerID, "Poor Campaign", 100_000_000, "poor-idem")
	spec.DaypartHours = []int16{}
	_, err = svc.CreateCampaign(ctx, spec)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient balance")

	_, err = pool.Exec(ctx, "UPDATE customers SET created_at = NOW() - INTERVAL '35 days' WHERE id = $1", ads.ToUUID(customerID))
	require.NoError(t, err)

	err = svc.TopUpBalance(ctx, customerID, 1_000_000_000, "topup-scoring-1")
	require.NoError(t, err)

	var balance int64
	err = pool.QueryRow(ctx, "SELECT balance FROM customers WHERE id = $1", customerID).Scan(&balance)
	require.NoError(t, err)
	assert.Equal(t, int64(1_000_000_000), balance)

	worker := NewCreditScoringWorker(svc)
	err = worker.EvaluateAll(ctx)
	require.NoError(t, err)

	var allowedOverdraft int64
	err = pool.QueryRow(ctx, "SELECT allowed_overdraft FROM customers WHERE id = $1", customerID).Scan(&allowedOverdraft)
	require.NoError(t, err)
	assert.Equal(t, int64(300_000_000), allowedOverdraft)

	_, err = pool.Exec(ctx, "UPDATE customers SET balance = 0 WHERE id = $1", ads.ToUUID(customerID))
	require.NoError(t, err)

	creditSpec := testCampaignSpec(customerID, "Credit Campaign", 200_000_000, "credit-idem")
	creditSpec.DaypartHours = []int16{}
	campaignID, err := svc.CreateCampaign(ctx, creditSpec)
	require.NoError(t, err)
	assert.NotEqual(t, uuid.Nil, campaignID)

	err = pool.QueryRow(ctx, "SELECT balance FROM customers WHERE id = $1", customerID).Scan(&balance)
	require.NoError(t, err)
	assert.Equal(t, int64(-200_000_000), balance)

	excessiveSpec := testCampaignSpec(customerID, "Excessive Campaign", 150_000_000, "excessive-idem")
	excessiveSpec.DaypartHours = []int16{}
	_, err = svc.CreateCampaign(ctx, excessiveSpec)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient balance")

	err = svc.UpdateOverdraft(ctx, customerID, 100_000_000)
	require.NoError(t, err)

	var campaignStatus string
	err = pool.QueryRow(ctx, "SELECT status::TEXT FROM campaigns WHERE id = $1", ads.ToUUID(campaignID)).Scan(&campaignStatus)
	require.NoError(t, err)
	assert.Equal(t, "PAUSED", campaignStatus)

	err = pool.QueryRow(ctx, "SELECT balance FROM customers WHERE id = $1", customerID).Scan(&balance)
	require.NoError(t, err)
	assert.Equal(t, int64(0), balance)

	err = pool.QueryRow(ctx, "SELECT allowed_overdraft FROM customers WHERE id = $1", customerID).Scan(&allowedOverdraft)
	require.NoError(t, err)
	assert.Equal(t, int64(100_000_000), allowedOverdraft)

	var outboxCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE event_type = 'PAUSE_CAMPAIGN'").Scan(&outboxCount)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, outboxCount, 1)
}
