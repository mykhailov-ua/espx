package management

import (
	"context"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/internal/config"
	"espx/internal/database"
	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClosedLoopPacingController guards pacing controller switches EVEN and ASAP from spend versus daily budget.
func TestClosedLoopPacingController(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		CampaignUpdateChannel: "test:pacing-updates",
	}

	sharder := ads.NewJumpHashSharder(1)
	svc := NewService(pool, []redis.UniversalClient{rdb}, sharder, cfg)
	defer svc.Close()

	ctx := context.Background()
	customerID := uuid.New()

	err := svc.CreateCustomer(ctx, customerID, "Pacing Customer", 1_000_000_000, "USD")
	require.NoError(t, err)

	campaignID, err := svc.CreateCampaign(ctx, CampaignCreateSpec{
		CustomerID: customerID, Name: "Pacing Test", BudgetLimit: 100_000_000,
		PacingMode: db.PacingModeTypeEVEN, DailyBudget: 100_000_000, Timezone: "UTC", FreqWindow: 86400, IdempotencyKey: "pacing-idem",
	})
	require.NoError(t, err)

	_, err = pool.Exec(ctx, "DELETE FROM outbox_events")
	require.NoError(t, err)

	queries := db.New(pool)
	campaignRepo := ads.NewCampaignRepo(queries)
	customerRepo := ads.NewCustomerRepo(queries)
	syncWorker := ads.NewSyncWorker(rdb, campaignRepo, customerRepo, 100*time.Millisecond)

	_, err = pool.Exec(ctx, "UPDATE campaigns SET current_spend = 50000, pacing_mode = 'EVEN' WHERE id = $1", ads.ToUUID(campaignID))
	require.NoError(t, err)

	err = svc.ClosedLoopPacingController(ctx, []*ads.SyncWorker{syncWorker})
	require.NoError(t, err)

	var pacing db.PacingModeType
	err = pool.QueryRow(ctx, "SELECT pacing_mode FROM campaigns WHERE id = $1", ads.ToUUID(campaignID)).Scan(&pacing)
	require.NoError(t, err)
	assert.Equal(t, db.PacingModeTypeASAP, pacing)

	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE event_type = 'UPDATE_CAMPAIGN_PACING'").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	_, err = pool.Exec(ctx, "DELETE FROM outbox_events")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, "UPDATE campaigns SET current_spend = 150000000, pacing_mode = 'ASAP' WHERE id = $1", ads.ToUUID(campaignID))
	require.NoError(t, err)

	err = svc.ClosedLoopPacingController(ctx, []*ads.SyncWorker{syncWorker})
	require.NoError(t, err)

	err = pool.QueryRow(ctx, "SELECT pacing_mode FROM campaigns WHERE id = $1", ads.ToUUID(campaignID)).Scan(&pacing)
	require.NoError(t, err)
	assert.Equal(t, db.PacingModeTypeEVEN, pacing)

	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE event_type = 'UPDATE_CAMPAIGN_PACING'").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// TestClosedLoopPacingController_EdgeCases guards pacing controller handles invalid timezone and zero budget safely.
func TestClosedLoopPacingController_EdgeCases(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		CampaignUpdateChannel: "test:pacing-updates",
	}

	sharder := ads.NewJumpHashSharder(1)
	svc := NewService(pool, []redis.UniversalClient{rdb}, sharder, cfg)
	defer svc.Close()

	ctx := context.Background()
	customerID := uuid.New()

	err := svc.CreateCustomer(ctx, customerID, "Pacing Customer Edge", 1_000_000_000, "USD")
	require.NoError(t, err)

	campaignID1, err := svc.CreateCampaign(ctx, CampaignCreateSpec{
		CustomerID: customerID, Name: "Pacing Timezone Edge", BudgetLimit: 100_000_000,
		PacingMode: db.PacingModeTypeEVEN, DailyBudget: 100_000_000, Timezone: "Invalid/Zone", FreqWindow: 86400, IdempotencyKey: "pacing-idem-1",
	})
	require.NoError(t, err)

	campaignID2, err := svc.CreateCampaign(ctx, CampaignCreateSpec{
		CustomerID: customerID, Name: "Pacing Zero Budget Edge", BudgetLimit: 0,
		PacingMode: db.PacingModeTypeEVEN, DailyBudget: 0, Timezone: "UTC", FreqWindow: 86400, IdempotencyKey: "pacing-idem-2",
	})
	require.NoError(t, err)

	_, err = pool.Exec(ctx, "DELETE FROM outbox_events")
	require.NoError(t, err)

	_, err = pool.Exec(ctx, "UPDATE campaigns SET current_spend = 50000, pacing_mode = 'EVEN' WHERE id = $1", ads.ToUUID(campaignID1))
	require.NoError(t, err)

	queries := db.New(pool)
	campaignRepo := ads.NewCampaignRepo(queries)
	customerRepo := ads.NewCustomerRepo(queries)
	syncWorker := ads.NewSyncWorker(rdb, campaignRepo, customerRepo, 100*time.Millisecond)

	err = svc.ClosedLoopPacingController(ctx, []*ads.SyncWorker{syncWorker})
	require.NoError(t, err)

	var pacing1 db.PacingModeType
	err = pool.QueryRow(ctx, "SELECT pacing_mode FROM campaigns WHERE id = $1", ads.ToUUID(campaignID1)).Scan(&pacing1)
	require.NoError(t, err)
	assert.Equal(t, db.PacingModeTypeASAP, pacing1)

	var pacing2 db.PacingModeType
	err = pool.QueryRow(ctx, "SELECT pacing_mode FROM campaigns WHERE id = $1", ads.ToUUID(campaignID2)).Scan(&pacing2)
	require.NoError(t, err)
	assert.Equal(t, db.PacingModeTypeEVEN, pacing2)
}

// BenchmarkClosedLoopPacingController measures closed-loop pacing tick cost across multiple campaigns.
func BenchmarkClosedLoopPacingController(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping integration benchmark")
	}

	pool, cleanupDB := database.SetupTestDB(b)
	defer cleanupDB()

	rdb, cleanupRedis := database.SetupTestRedis(b)
	defer cleanupRedis()

	cfg := &config.Config{
		CampaignUpdateChannel: "test:pacing-updates",
	}

	sharder := ads.NewJumpHashSharder(1)
	svc := NewService(pool, []redis.UniversalClient{rdb}, sharder, cfg)
	defer svc.Close()

	ctx := context.Background()
	customerID := uuid.New()

	err := svc.CreateCustomer(ctx, customerID, "Bench Pacing Customer", 1_000_000_000, "USD")
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		_, err := svc.CreateCampaign(ctx, CampaignCreateSpec{
			CustomerID: customerID, Name: uuid.New().String(), BudgetLimit: 100_000_000,
			PacingMode: db.PacingModeTypeEVEN, DailyBudget: 100_000_000, Timezone: "UTC", FreqWindow: 86400, IdempotencyKey: uuid.New().String(),
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	queries := db.New(pool)
	campaignRepo := ads.NewCampaignRepo(queries)
	customerRepo := ads.NewCustomerRepo(queries)
	syncWorker := ads.NewSyncWorker(rdb, campaignRepo, customerRepo, 100*time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err = svc.ClosedLoopPacingController(ctx, []*ads.SyncWorker{syncWorker})
		if err != nil {
			b.Fatal(err)
		}
	}
}
