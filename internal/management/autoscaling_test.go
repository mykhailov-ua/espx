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

// TestSmartBudgetAutoscaling verifies the coordination and execution of the smart budget autoscaling worker.
// It sets up two campaigns for a single customer, simulates un-synchronized spends in Redis, updates stats to establish CTR variance,
// triggers autoscaling with SyncWorker registry flushing, and asserts atomic budget reallocations and outbox event emissions.
func TestSmartBudgetAutoscaling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		CampaignUpdateChannel: "test:autoscaling-updates",
	}

	sharder := ads.NewJumpHashSharder(1)
	svc := NewService(pool, []redis.UniversalClient{rdb}, sharder, cfg)
	defer svc.Close()

	ctx := context.Background()
	customerID := uuid.New()

	// 1. Create customer with sufficient initial balance.
	err := svc.CreateCustomer(ctx, customerID, "Smart Customer", 1_000_000_000, "USD")
	require.NoError(t, err)

	// 2. Create low-performer campaign (A).
	campaignA, err := svc.CreateCampaign(ctx, customerID, nil, "Low CTR Campaign", 100_000_000, db.PacingModeTypeASAP, 0, "UTC", 0, 0, nil, "low-idem")
	require.NoError(t, err)

	// 3. Create high-performer campaign (B).
	campaignB, err := svc.CreateCampaign(ctx, customerID, nil, "High CTR Campaign", 100_000_000, db.PacingModeTypeASAP, 0, "UTC", 0, 0, nil, "high-idem")
	require.NoError(t, err)

	// 4. Populate performance statistics to establish CTR differences:
	// Campaign A (Low Performer): 1000 impressions, 2 clicks -> 0.2% CTR (< 0.5%)
	_, err = pool.Exec(ctx,
		"INSERT INTO campaign_stats (campaign_id, date, impressions_count, clicks_count, conversions_count) VALUES ($1, CURRENT_DATE, 1000, 2, 0)",
		ads.ToUUID(campaignA),
	)
	require.NoError(t, err)

	// Campaign B (High Performer): 500 impressions, 15 clicks -> 3.0% CTR (> 1.5%)
	_, err = pool.Exec(ctx,
		"INSERT INTO campaign_stats (campaign_id, date, impressions_count, clicks_count, conversions_count) VALUES ($1, CURRENT_DATE, 500, 15, 0)",
		ads.ToUUID(campaignB),
	)
	require.NoError(t, err)

	// 5. Simulate active un-flushed budget spend in Redis for Campaign A.
	// We record $5.00 (5,000,000 micro-units) spent on Campaign A in Redis to test process coordination.
	err = rdb.SAdd(ctx, "budget:dirty_campaigns", campaignA.String()).Err()
	require.NoError(t, err)
	err = rdb.Set(ctx, "budget:sync:campaign:"+campaignA.String(), 5000000, 0).Err()
	require.NoError(t, err)

	// 6. Build SyncWorker referencing the postgres store and Redis client.
	queries := db.New(pool)
	campaignRepo := ads.NewCampaignRepo(queries)
	customerRepo := ads.NewCustomerRepo(queries)
	syncWorker := ads.NewSyncWorker(rdb, campaignRepo, customerRepo, 100*time.Millisecond)

	// Set initial keys in Redis to simulate cached budgets before eviction.
	err = rdb.Set(ctx, "budget:campaign:"+campaignA.String(), 100000000, 0).Err()
	require.NoError(t, err)
	err = rdb.Set(ctx, "budget:campaign:"+campaignB.String(), 100000000, 0).Err()
	require.NoError(t, err)

	// Clear outbox events prior to evaluation.
	_, err = pool.Exec(ctx, "DELETE FROM outbox_events")
	require.NoError(t, err)

	// 7. Execute budget autoscaler with synchronous process coordination.
	err = svc.AutoscaleBudgets(ctx, []*ads.SyncWorker{syncWorker})
	require.NoError(t, err)

	// 8. Assert PostgreSQL spends and limits are properly updated:
	// Spends check: Campaign A's $5.00 Redis spend must have been successfully synchronized to Postgres first.
	var spendA string
	err = pool.QueryRow(ctx, "SELECT current_spend::TEXT FROM campaigns WHERE id = $1", ads.ToUUID(campaignA)).Scan(&spendA)
	require.NoError(t, err)
	assert.Equal(t, "5000000", spendA)

	// Budget limit check: Campaign A limit should decrease to $90.00 ($100 - $10).
	var limitA string
	err = pool.QueryRow(ctx, "SELECT budget_limit::TEXT FROM campaigns WHERE id = $1", ads.ToUUID(campaignA)).Scan(&limitA)
	require.NoError(t, err)
	assert.Equal(t, "90000000", limitA)

	// Budget limit check: Campaign B limit should increase to $110.00 ($100 + $10).
	var limitB string
	err = pool.QueryRow(ctx, "SELECT budget_limit::TEXT FROM campaigns WHERE id = $1", ads.ToUUID(campaignB)).Scan(&limitB)
	require.NoError(t, err)
	assert.Equal(t, "110000000", limitB)

	// 9. Verify outbox events: two CREATE_CAMPAIGN events must be generated to update Redis caches.
	var outboxCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE event_type = 'CREATE_CAMPAIGN'").Scan(&outboxCount)
	require.NoError(t, err)
	assert.Equal(t, 2, outboxCount)

	// Verify that syncWorker committing successfully cleaned up redis dirty sets and sync keys.
	val, err := rdb.Get(ctx, "budget:sync:campaign:"+campaignA.String()).Result()
	assert.Equal(t, redis.Nil, err)
	assert.Empty(t, val)
}
