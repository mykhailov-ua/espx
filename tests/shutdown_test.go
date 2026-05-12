package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/redis/go-redis/v9"
	ads_delivery "github.com/mykhailov-ua/ad-event-processor/internal/ads/delivery"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/repository"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/database"
	infra_repo "github.com/mykhailov-ua/ad-event-processor/internal/infra/repository"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGracefulShutdown_NoDataLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := setupTestDB(t)
	defer cleanupDB()

	rdb, cleanupRedis := setupTestRedis(t)
	defer cleanupRedis()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queries := repository.New(pool)
	cfg := &config.Config{
		EventBatchSize:     10,
		EventFlushMs:       100,
		StatsFlushMs:       100,
		MaxWorkers:         2,
		WriteTimeoutMs:     5000,
		MaxRequestBodySize: 1024 * 1024,
		StreamMaxLen:       100000,
	}

	pm := database.NewPartitionManager(pool, 7, 1)
	require.NoError(t, pm.Run(ctx))

	customerID := uuid.New()
	_, _ = pool.Exec(ctx, "INSERT INTO customers (id, name, balance) VALUES ($1, $2, $3)", customerID, "Shutdown Customer", 1000.00)

	campaignID := uuid.New()
	_, err := pool.Exec(ctx, "INSERT INTO campaigns (id, name, status, customer_id, budget_limit) VALUES ($1, $2, $3, $4, $5)", campaignID, "Shutdown Test", "ACTIVE", customerID, 1000.00)
	require.NoError(t, err)

	registry := ads.NewRegistry(queries)
	_, _ = registry.Sync(ctx)

	store := ads.NewPostgresStore(queries, 5*time.Second)
	campaignRepo := infra_repo.NewCampaignRepo(queries)
	unifiedFilter := ads.NewUnifiedFilter(
		[]redis.UniversalClient{rdb},
		registry,
		campaignRepo,
		1000,
		time.Minute,
		45*time.Second,
		24*time.Hour,
		0.1,
		0.01,
		"shutdown-stream",
		100000,
	)
	filterEngine := ads.NewFilterEngine(unifiedFilter)
	consumer := ads.NewStreamConsumer(store, rdb, "shutdown-stream", "shutdown-group", "shutdown-c1", cfg.EventBatchSize, cfg.MaxWorkers, 100*time.Millisecond, 5*time.Second, 100*time.Millisecond, 5*time.Second, 5, 5*time.Minute, 1*time.Second)
	consumer.Start(ctx)

	router := ads_delivery.NewRouter(cfg, registry, filterEngine, pool, []redis.UniversalClient{rdb})
	srv := httptest.NewServer(router)
	defer srv.Close()

	const eventCount = 50
	var wg sync.WaitGroup
	var acceptedCount int64
	var mu sync.Mutex

	for i := 0; i < eventCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			payload := map[string]any{
				"campaign_id": campaignID,
				"type":        "click",
				"payload":     map[string]string{"idx": fmt.Sprintf("%d", idx)},
			}
			body, _ := json.Marshal(payload)
			resp, err := http.Post(srv.URL+"/track", "application/json", bytes.NewBuffer(body))
			if err == nil && resp.StatusCode == http.StatusAccepted {
				mu.Lock()
				acceptedCount++
				mu.Unlock()
			}
			if resp != nil {
				resp.Body.Close()
			}
		}(i)
	}

	wg.Wait()
	require.Equal(t, int64(eventCount), acceptedCount)

	consumer.Close()
	consumer.Wait(ctx)

	cancel()

	assert.Eventually(t, func() bool {
		var dbEventCount int64
		err = pool.QueryRow(context.Background(), "SELECT count(*) FROM events WHERE campaign_id = $1", campaignID).Scan(&dbEventCount)
		return err == nil && dbEventCount == acceptedCount
	}, 15*time.Second, 100*time.Millisecond, "All accepted events should be persisted to database")
}
