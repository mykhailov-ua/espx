package management

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/internal/config"
	"espx/internal/database"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestOutboxPerformanceMetrics(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping outbox performance metrics in short mode")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		CampaignUpdateChannel: "campaigns:update-test",
	}
	svc := NewService(pool, []redis.UniversalClient{rdb}, ads.NewJumpHashSharder(1), cfg)
	svc.Close()

	ctx := context.Background()
	queries := db.New(pool)

	const eventCount = 100

	t.Log("MEASURING TRANSACTION TIMES: Standard Outbox vs Decoupled Outbox")

	seedEvents(t, queries, eventCount)

	worker := NewOutboxWorker(svc)

	start := time.Now()
	err := worker.ProcessOutbox(ctx)
	require.NoError(t, err)
	durationNormal := time.Since(start)
	t.Logf("[Baseline Redis] Processed %d events in standard outbox loop.", eventCount)
	t.Logf("-> Active PG Transaction Duration: %v (%.3f ms/op)", durationNormal, float64(durationNormal.Nanoseconds())/1e6/float64(eventCount))

	t.Log("\nSIMULATING LOCK CONTENTION & CONNECTION STARVATION UNDER REDIS LATENCY (50ms)")

	seedEvents(t, queries, 10)

	var wg sync.WaitGroup
	wg.Add(2)

	var tx1Start, tx1End, tx2Start, tx2End time.Time
	lockedSignal := make(chan struct{})

	go func() {
		defer wg.Done()
		tx1Start = time.Now()
		_ = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			q := db.New(tx)
			events, err := q.GetPendingOutboxEventsForUpdate(ctx, 10)
			if err != nil {
				return err
			}

			close(lockedSignal)

			time.Sleep(50 * time.Millisecond)

			for _, ev := range events {
				_ = q.MarkOutboxEventProcessed(ctx, ev.ID)
			}
			return nil
		})
		tx1End = time.Now()
	}()

	go func() {
		defer wg.Done()

		select {
		case <-lockedSignal:
		case <-time.After(2 * time.Second):
		}

		tx2Start = time.Now()
		_ = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {

			_, _ = tx.Exec(ctx, "SELECT id FROM outbox_events WHERE status = 'PENDING' FOR UPDATE")
			return nil
		})
		tx2End = time.Now()
	}()

	wg.Wait()

	t.Logf("Worker 1 Transaction Hold Time (Simulated Redis Delay): %v", tx1End.Sub(tx1Start))
	t.Logf("Worker 2 Transaction Blocked Duration (Waiting for Locks): %v", tx2End.Sub(tx2Start))
	require.True(t, tx2End.Sub(tx2Start) >= 30*time.Millisecond, "Worker 2 should have been blocked waiting for Worker 1's lock release")
}

func seedEvents(t *testing.T, queries *db.Queries, count int) {
	ctx := context.Background()
	for i := 0; i < count; i++ {
		payload := CampaignPayload{
			CampaignID:  uuid.New().String(),
			BudgetLimit: 100_500_000,
		}
		payloadBytes, err := json.Marshal(payload)
		require.NoError(t, err)

		_, err = queries.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
			EventType: "CREATE_CAMPAIGN",
			Payload:   payloadBytes,
		})
		require.NoError(t, err)
	}
}
