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

func TestSmartBudgetAutoscaling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)

	rdb, cleanupRedis := database.SetupTestRedis(t)

	cfg := &config.Config{
		CampaignUpdateChannel:       "test:autoscaling-updates",
		AutoscaleHighCTRThreshold:   0.015,
		AutoscaleMinImpressions:     100,
		AutoscaleLowCTRThreshold:    0.005,
		AutoscaleMinRemainingBudget: 20_000_000,
		AutoscaleShiftAmount:        10_000_000,
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

	err := svc.CreateCustomer(ctx, customerID, "Smart Customer", 1_000_000_000, "USD")
	require.NoError(t, err)

	campaignA, err := svc.CreateCampaign(ctx, customerID, nil, "Low CTR Campaign", 100_000_000, db.PacingModeTypeASAP, 0, "UTC", 0, 0, nil, "low-idem")
	require.NoError(t, err)

	campaignB, err := svc.CreateCampaign(ctx, customerID, nil, "High CTR Campaign", 100_000_000, db.PacingModeTypeASAP, 0, "UTC", 0, 0, nil, "high-idem")
	require.NoError(t, err)

	_, err = pool.Exec(ctx,
		"INSERT INTO campaign_stats (campaign_id, date, impressions_count, clicks_count, conversions_count) VALUES ($1, CURRENT_DATE, 1000, 2, 0)",
		ads.ToUUID(campaignA),
	)
	require.NoError(t, err)

	_, err = pool.Exec(ctx,
		"INSERT INTO campaign_stats (campaign_id, date, impressions_count, clicks_count, conversions_count) VALUES ($1, CURRENT_DATE, 500, 15, 0)",
		ads.ToUUID(campaignB),
	)
	require.NoError(t, err)

	err = rdb.SAdd(ctx, "budget:dirty_campaigns", campaignA.String()).Err()
	require.NoError(t, err)
	err = rdb.Set(ctx, "budget:sync:campaign:"+campaignA.String(), 5000000, 0).Err()
	require.NoError(t, err)

	queries := db.New(pool)
	campaignRepo := ads.NewCampaignRepo(queries)
	customerRepo := ads.NewCustomerRepo(queries)
	syncWorker := ads.NewSyncWorker(rdb, campaignRepo, customerRepo, 100*time.Millisecond)

	err = rdb.Set(ctx, "budget:campaign:"+campaignA.String(), 100000000, 0).Err()
	require.NoError(t, err)
	err = rdb.Set(ctx, "budget:campaign:"+campaignB.String(), 100000000, 0).Err()
	require.NoError(t, err)

	_, err = pool.Exec(ctx, "DELETE FROM outbox_events")
	require.NoError(t, err)

	err = svc.AutoscaleBudgets(ctx, []*ads.SyncWorker{syncWorker})
	require.NoError(t, err)

	var spendA string
	err = pool.QueryRow(ctx, "SELECT current_spend::TEXT FROM campaigns WHERE id = $1", ads.ToUUID(campaignA)).Scan(&spendA)
	require.NoError(t, err)
	assert.Equal(t, "5000000", spendA)

	var limitA string
	err = pool.QueryRow(ctx, "SELECT budget_limit::TEXT FROM campaigns WHERE id = $1", ads.ToUUID(campaignA)).Scan(&limitA)
	require.NoError(t, err)
	assert.Equal(t, "90000000", limitA)

	var limitB string
	err = pool.QueryRow(ctx, "SELECT budget_limit::TEXT FROM campaigns WHERE id = $1", ads.ToUUID(campaignB)).Scan(&limitB)
	require.NoError(t, err)
	assert.Equal(t, "110000000", limitB)

	var outboxCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE event_type = 'CREATE_CAMPAIGN'").Scan(&outboxCount)
	require.NoError(t, err)
	assert.Equal(t, 2, outboxCount)

	val, err := rdb.Get(ctx, "budget:sync:campaign:"+campaignA.String()).Result()
	assert.Equal(t, redis.Nil, err)
	assert.Empty(t, val)
}
