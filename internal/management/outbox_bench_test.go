package management

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/database"
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
	svc.Close() // Stop background workers immediately to avoid them stealing seeded events

	ctx := context.Background()
	queries := db.New(pool)

	const eventCount = 100

	// 1. Measure standard processing duration with real Redis (localhost)
	t.Log("--------------------------------------------------------------------------------")
	t.Log("MEASURING TRANSACTION TIMES: Standard Outbox vs Decoupled Outbox")
	t.Log("--------------------------------------------------------------------------------")

	// Seed events
	seedEvents(t, queries, eventCount)

	worker := NewOutboxWorker(svc)

	start := time.Now()
	err := worker.ProcessOutbox(ctx)
	require.NoError(t, err)
	durationNormal := time.Since(start)
	t.Logf("[Baseline Redis] Processed %d events in standard outbox loop.", eventCount)
	t.Logf("-> Active PG Transaction Duration: %v (%.3f ms/op)", durationNormal, float64(durationNormal.Nanoseconds())/1e6/float64(eventCount))

	// 2. Simulate Connection Pool Starvation & Row Lock Contention under Latency
	// If the OutboxWorker gets stuck on a slow Redis call (e.g. 50ms latency),
	// let's measure how long another worker or the API pool is blocked on the locked rows.
	t.Log("\n--------------------------------------------------------------------------------")
	t.Log("SIMULATING LOCK CONTENTION & CONNECTION STARVATION UNDER REDIS LATENCY (50ms)")
	t.Log("--------------------------------------------------------------------------------")

	seedEvents(t, queries, 10)

	var wg sync.WaitGroup
	wg.Add(2)

	var tx1Start, tx1End, tx2Start, tx2End time.Time
	lockedSignal := make(chan struct{})

	// Worker 1: Locks events and simulates a slow Redis write (50ms sleep) inside PG transaction
	go func() {
		defer wg.Done()
		tx1Start = time.Now()
		_ = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			q := db.New(tx)
			events, err := q.GetPendingOutboxEventsForUpdate(ctx, 10)
			if err != nil {
				return err
			}
			// Signal Worker 2 that locks have been successfully acquired
			close(lockedSignal)

			// Simulate Redis write latency of 50ms inside the transaction
			time.Sleep(50 * time.Millisecond)

			for _, ev := range events {
				_ = q.MarkOutboxEventProcessed(ctx, ev.ID)
			}
			return nil
		})
		tx1End = time.Now()
	}()

	// Worker 2: Tries to acquire the SAME outbox events using FOR UPDATE WITHOUT SKIP LOCKED
	// to measure the exact blocking latency.
	go func() {
		defer wg.Done()
		// Wait until Worker 1 has locked the events
		select {
		case <-lockedSignal:
		case <-time.After(2 * time.Second):
		}

		tx2Start = time.Now()
		_ = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			// Query for update WITHOUT skip locked to block on the same rows
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
