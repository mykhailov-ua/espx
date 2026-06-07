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

	campaignID, err := svc.CreateCampaign(ctx, customerID, nil, "Pacing Test", 100_000_000, db.PacingModeTypeEVEN, 100_000_000, "UTC", 0, 0, nil, "pacing-idem")
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

	campaignID1, err := svc.CreateCampaign(ctx, customerID, nil, "Pacing Timezone Edge", 100_000_000, db.PacingModeTypeEVEN, 100_000_000, "Invalid/Zone", 0, 0, nil, "pacing-idem-1")
	require.NoError(t, err)

	campaignID2, err := svc.CreateCampaign(ctx, customerID, nil, "Pacing Zero Budget Edge", 0, db.PacingModeTypeEVEN, 0, "UTC", 0, 0, nil, "pacing-idem-2")
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
