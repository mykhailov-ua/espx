package management

import (
	"context"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/internal/database"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProcessScheduleTickSkipsAlreadyAligned guards schedule tick is no-op when campaign status already matches window.
func TestProcessScheduleTickSkipsAlreadyAligned(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	svc := NewService(pool, []redis.UniversalClient{rdb}, ads.NewJumpHashSharder(1), nil)
	defer svc.Close()

	ctx := context.Background()
	custID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, custID, "Sched Tick", 300_000_000, "USD"))

	start := time.Now().Add(-time.Hour)
	end := time.Now().Add(24 * time.Hour)
	spec := testCampaignSpec(custID, "Window", 50_000_000, "sched-tick-idem")
	spec.StartAt = &start
	spec.EndAt = &end
	spec.DaypartHours = []int16{}
	campID, err := svc.CreateCampaign(ctx, spec)
	require.NoError(t, err)

	camp, err := svc.GetCampaign(ctx, campID)
	require.NoError(t, err)
	assert.Equal(t, db.CampaignStatusTypeACTIVE, camp.Status)

	require.NoError(t, svc.ProcessScheduleTick(ctx))

	camp, err = svc.GetCampaign(ctx, campID)
	require.NoError(t, err)
	assert.Equal(t, db.CampaignStatusTypeACTIVE, camp.Status)
}

// TestCreateCampaignRejectsIncompleteIdempotencyLedger guards create campaign rejects orphaned FREEZE ledger rows.
func TestCreateCampaignRejectsIncompleteIdempotencyLedger(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	svc := NewService(pool, nil, nil, nil)
	defer svc.Close()

	ctx := context.Background()
	custID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, custID, "Idem", 500_000_000, "USD"))

	key := "broken-idem-key"
	_, err := pool.Exec(ctx, `
		INSERT INTO balance_ledger (customer_id, amount, type, idempotency_hash)
		VALUES ($1, $2, 'FREEZE', $3)`,
		ads.ToUUID(custID), int64(10_000_000), key)
	require.NoError(t, err)

	_, err = svc.CreateCampaign(ctx, testCampaignSpec(custID, "Broken", 20_000_000, key))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "incomplete idempotency")
}

// TestServiceCloseGuardsLateWorkerStart guards worker start after Close returns immediately without blocking.
func TestServiceCloseGuardsLateWorkerStart(t *testing.T) {
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	svc := NewService(pool, nil, nil, nil)
	svc.Close()

	done := make(chan struct{})
	go func() {
		svc.StartReconWorker(time.Minute)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("StartReconWorker blocked after Close")
	}
}

// TestUpdateSettingsOutboxVersionIsIdempotent guards reprocessing settings outbox keeps config:version stable.
func TestUpdateSettingsOutboxVersionIsIdempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, nil)

	ctx := context.Background()
	require.NoError(t, svc.UpdateSettings(ctx, map[string]string{"rate_limit_per_min": "42"}))

	worker := NewOutboxWorker(svc)
	processed, err := worker.ProcessOutboxWithCount(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, 1, processed)

	var eventID int64
	err = pool.QueryRow(ctx, `SELECT id FROM outbox_events WHERE event_type = 'UPDATE_SETTINGS' ORDER BY id DESC LIMIT 1`).Scan(&eventID)
	require.NoError(t, err)

	version, err := rdb.Get(ctx, "config:version").Int64()
	require.NoError(t, err)
	assert.Equal(t, eventID, version)

	_, err = pool.Exec(ctx, `UPDATE outbox_events SET status = 'PENDING', processing_started_at = NULL WHERE id = $1`, eventID)
	require.NoError(t, err)

	processed, err = worker.ProcessOutboxWithCount(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, 1, processed)

	versionAfter, err := rdb.Get(ctx, "config:version").Int64()
	require.NoError(t, err)
	assert.Equal(t, eventID, versionAfter)
}
