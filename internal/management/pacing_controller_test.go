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
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	// Create campaign in EVEN pacing mode
	campaignID, err := svc.CreateCampaign(ctx, customerID, nil, "Pacing Test", 100_000_000, db.PacingModeTypeEVEN, 100_000_000, "UTC", 0, 0, nil, "pacing-idem")
	require.NoError(t, err)

	// Clear outbox events prior to evaluation
	_, err = pool.Exec(ctx, "DELETE FROM outbox_events")
	require.NoError(t, err)

	// Setup mock sync worker
	queries := db.New(pool)
	campaignRepo := ads.NewCampaignRepo(queries)
	customerRepo := ads.NewCustomerRepo(queries)
	syncWorker := ads.NewSyncWorker(rdb, campaignRepo, customerRepo, 100*time.Millisecond)

	// Case 1: Under-spending scenario
	// Expect daily budget is 100. Expected spend at local day (which is at least some ratio)
	// Let's force campaigns time to have some elapsed time. If it is 12:00 UTC (ratio 0.5), expected spend is 50.
	// But let's check: we can cheat the expected spend in the test by updating current_spend to a tiny value (e.g. 0.05).
	// Because expected spend will be at least a positive value (since time ratios are calculated using time.Now()),
	// a current_spend of 0.00 or 0.05 will always be under the underThreshold!
	_, err = pool.Exec(ctx, "UPDATE campaigns SET current_spend = 50000, pacing_mode = 'EVEN' WHERE id = $1", ads.ToUUID(campaignID))
	require.NoError(t, err)

	// Run pacing controller
	err = svc.ClosedLoopPacingController(ctx, []*ads.SyncWorker{syncWorker})
	require.NoError(t, err)

	// Assert pacing mode changed to ASAP
	var pacing db.PacingModeType
	err = pool.QueryRow(ctx, "SELECT pacing_mode FROM campaigns WHERE id = $1", ads.ToUUID(campaignID)).Scan(&pacing)
	require.NoError(t, err)
	assert.Equal(t, db.PacingModeTypeASAP, pacing)

	// Verify that the Outbox event was created
	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE event_type = 'UPDATE_CAMPAIGN_PACING'").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Case 2: Over-spending scenario
	// Set current spend to a huge value (e.g. 150.00), pacing mode back to ASAP
	_, err = pool.Exec(ctx, "DELETE FROM outbox_events")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, "UPDATE campaigns SET current_spend = 150000000, pacing_mode = 'ASAP' WHERE id = $1", ads.ToUUID(campaignID))
	require.NoError(t, err)

	// Run pacing controller
	err = svc.ClosedLoopPacingController(ctx, []*ads.SyncWorker{syncWorker})
	require.NoError(t, err)

	// Assert pacing mode changed to EVEN
	err = pool.QueryRow(ctx, "SELECT pacing_mode FROM campaigns WHERE id = $1", ads.ToUUID(campaignID)).Scan(&pacing)
	require.NoError(t, err)
	assert.Equal(t, db.PacingModeTypeEVEN, pacing)

	// Verify that the Outbox event was created
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE event_type = 'UPDATE_CAMPAIGN_PACING'").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

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

	// Campaign 1: Invalid timezone
	campaignID1, err := svc.CreateCampaign(ctx, customerID, nil, "Pacing Timezone Edge", 100_000_000, db.PacingModeTypeEVEN, 100_000_000, "Invalid/Zone", 0, 0, nil, "pacing-idem-1")
	require.NoError(t, err)

	// Campaign 2: Zero budget
	campaignID2, err := svc.CreateCampaign(ctx, customerID, nil, "Pacing Zero Budget Edge", 0, db.PacingModeTypeEVEN, 0, "UTC", 0, 0, nil, "pacing-idem-2")
	require.NoError(t, err)

	// Clear outbox
	_, err = pool.Exec(ctx, "DELETE FROM outbox_events")
	require.NoError(t, err)

	// Force spending on Campaign 1 (invalid timezone -> should fall back to UTC and pacing should execute)
	_, err = pool.Exec(ctx, "UPDATE campaigns SET current_spend = 50000, pacing_mode = 'EVEN' WHERE id = $1", ads.ToUUID(campaignID1))
	require.NoError(t, err)

	// Setup mock sync worker
	queries := db.New(pool)
	campaignRepo := ads.NewCampaignRepo(queries)
	customerRepo := ads.NewCustomerRepo(queries)
	syncWorker := ads.NewSyncWorker(rdb, campaignRepo, customerRepo, 100*time.Millisecond)

	// Run pacing controller
	err = svc.ClosedLoopPacingController(ctx, []*ads.SyncWorker{syncWorker})
	require.NoError(t, err)

	// Assert pacing mode for Campaign 1 changed to ASAP (since it successfully fell back to UTC)
	var pacing1 db.PacingModeType
	err = pool.QueryRow(ctx, "SELECT pacing_mode FROM campaigns WHERE id = $1", ads.ToUUID(campaignID1)).Scan(&pacing1)
	require.NoError(t, err)
	assert.Equal(t, db.PacingModeTypeASAP, pacing1)

	// Assert pacing mode for Campaign 2 remains EVEN (since it was skipped due to zero budget)
	var pacing2 db.PacingModeType
	err = pool.QueryRow(ctx, "SELECT pacing_mode FROM campaigns WHERE id = $1", ads.ToUUID(campaignID2)).Scan(&pacing2)
	require.NoError(t, err)
	assert.Equal(t, db.PacingModeTypeEVEN, pacing2)
}

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

	// Create 10 active campaigns that do not require adjustments
	for i := 0; i < 10; i++ {
		_, err := svc.CreateCampaign(ctx, customerID, nil, uuid.New().String(), 100_000_000, db.PacingModeTypeEVEN, 100_000_000, "UTC", 0, 0, nil, uuid.New().String())
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
