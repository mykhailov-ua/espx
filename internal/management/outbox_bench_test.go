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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// TestOutboxPerformanceMetrics guards outbox polling serializes concurrent FOR UPDATE claimers.
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

	const eventCount = 100

	seedEvents(t, pool, eventCount)

	worker := NewOutboxWorker(svc)

	start := time.Now()
	err := worker.ProcessOutbox(ctx)
	require.NoError(t, err)
	durationNormal := time.Since(start)
	t.Logf("processed %d events in %v (%.3f ms/op)", eventCount, durationNormal, float64(durationNormal.Nanoseconds())/1e6/float64(eventCount))

	seedEvents(t, pool, 10)

	var wg sync.WaitGroup
	wg.Add(2)

	var tx1Start, tx1End, tx2Start, tx2End time.Time
	lockedSignal := make(chan struct{})
	tx2Started := make(chan struct{})

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

			select {
			case <-tx2Started:
			case <-time.After(2 * time.Second):
			}

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
			close(tx2Started)

			_, _ = tx.Exec(ctx, "SELECT id FROM outbox_events WHERE status = 'PENDING' FOR UPDATE")
			return nil
		})
		tx2End = time.Now()
	}()

	wg.Wait()

	t.Logf("worker 1 transaction hold time: %v", tx1End.Sub(tx1Start))
	t.Logf("worker 2 blocked duration: %v", tx2End.Sub(tx2Start))
	require.True(t, tx2End.Sub(tx2Start) >= 30*time.Millisecond, "Worker 2 should have been blocked waiting for Worker 1's lock release")
}

// seedEvents exists so outbox locking tests start from a known bulk pending backlog.
func seedEvents(t *testing.T, pool *pgxpool.Pool, count int) {
	ctx := context.Background()
	payloads := make([][]byte, count)
	for i := 0; i < count; i++ {
		payload := CampaignPayload{
			CampaignID:  uuid.New().String(),
			BudgetLimit: 100_500_000,
		}
		var err error
		payloads[i], err = json.Marshal(payload)
		require.NoError(t, err)
	}

	_, err := pool.Exec(ctx, `
		INSERT INTO outbox_events (event_type, payload)
		SELECT 'CREATE_CAMPAIGN', unnest($1::jsonb[])
	`, payloads)
	require.NoError(t, err)
}

// TestOutboxExplainAnalyze captures query plan for pending outbox claim to catch index regressions.
func TestOutboxExplainAnalyze(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping outbox EXPLAIN ANALYZE in short mode")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	ctx := context.Background()

	seedEvents(t, pool, 1000)

	row, err := pool.Query(ctx, `EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS)
SELECT id, event_type, payload, status, created_at FROM outbox_events
WHERE status = 'PENDING'
ORDER BY created_at ASC
LIMIT 1000
FOR UPDATE SKIP LOCKED;`)
	require.NoError(t, err)
	defer row.Close()

	for row.Next() {
		var planLine string
		err := row.Scan(&planLine)
		require.NoError(t, err)
		t.Log(planLine)
	}
}

// BenchmarkProcessOutbox measures outbox batch processing throughput.
func BenchmarkProcessOutbox(b *testing.B) {
	pool, cleanupDB := database.SetupTestDB(b)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(b)
	defer cleanupRedis()

	cfg := &config.Config{
		CampaignUpdateChannel: "campaigns:update-bench",
	}
	svc := NewService(pool, []redis.UniversalClient{rdb}, ads.NewJumpHashSharder(1), cfg)
	defer svc.Close()

	worker := NewOutboxWorker(svc)
	ctx := context.Background()

	b.StopTimer()
	seedEventsForBench(pool, b.N*1000)
	b.StartTimer()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := worker.ProcessOutboxWithCount(ctx, 1000)
		if err != nil {
			b.Fatalf("ProcessOutbox failed: %v", err)
		}
	}
}

// seedEventsForBench exists so outbox benchmarks avoid per-iteration row setup overhead.
func seedEventsForBench(pool *pgxpool.Pool, count int) {
	ctx := context.Background()
	const batchSize = 10000
	for i := 0; i < count; i += batchSize {
		currentBatch := batchSize
		if i+currentBatch > count {
			currentBatch = count - i
		}

		payloads := make([][]byte, currentBatch)
		for j := 0; j < currentBatch; j++ {
			payload := CampaignPayload{
				CampaignID:  uuid.New().String(),
				BudgetLimit: 100_500_000,
			}
			payloads[j], _ = json.Marshal(payload)
		}

		_, err := pool.Exec(ctx, `
			INSERT INTO outbox_events (event_type, payload)
			SELECT 'CREATE_CAMPAIGN', unnest($1::jsonb[])
		`, payloads)
		if err != nil {
			panic(err)
		}
	}
}
