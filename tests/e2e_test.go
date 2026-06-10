package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/internal/ads/pb"
	"espx/internal/config"
	"espx/internal/database"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestE2EFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := setupTestDB(t)
	defer cleanupDB()

	rdb, cleanupRedis := setupTestRedis(t)
	defer cleanupRedis()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queries := db.New(pool)
	cfg := &config.Config{
		EventBatchSize:     10,
		EventFlushMs:       100,
		StatsFlushMs:       100,
		MaxWorkers:         2,
		WriteTimeoutMs:     1000,
		FilterTimeoutMs:    1000,
		MaxRequestBodySize: 1024 * 1024,
		StreamMaxLen:       100000,
	}

	partManager := database.NewPartitionManager(pool, 7, 2)
	err := partManager.Run(ctx)
	require.NoError(t, err)

	customerID := uuid.New()
	_, err = pool.Exec(ctx, "INSERT INTO customers (id, name, balance) VALUES ($1, $2, $3)", customerID, "Test Customer", 1_000_000_000)
	require.NoError(t, err)

	campaignID := uuid.New()
	_, err = pool.Exec(ctx, "INSERT INTO campaigns (id, name, status, customer_id, budget_limit) VALUES ($1, $2, $3, $4, $5)", campaignID, "E2E Campaign", "ACTIVE", customerID, 100_000_000)
	require.NoError(t, err)

	registry := newTestRegistry(t, queries)
	_, _ = registry.Sync(ctx)

	store := ads.NewPostgresStore(queries, 1*time.Second)
	campaignRepo := ads.NewCampaignRepo(queries)
	unifiedFilter := ads.NewUnifiedFilter(
		[]redis.UniversalClient{rdb},
		ads.NewJumpHashSharder(1),
		registry,
		campaignRepo,
		1000,
		time.Minute,
		45*time.Second,
		24*time.Hour,
		100_000,
		10_000,
		"test-stream",
		100000,
	)
	filterEngine := ads.NewFilterEngine(unifiedFilter)
	consumer := ads.NewStreamConsumer(store, rdb, "test-stream", "test-group", "test-c1", cfg.EventBatchSize, cfg.MaxWorkers, 100*time.Millisecond, 1*time.Second, 100*time.Millisecond, 5*time.Second, 5, 5*time.Minute, 1*time.Second)
	consumer.Start(ctx)
	defer consumer.Close()

	sharder := ads.NewJumpHashSharder(1)
	router := ads.NewRouter(cfg, registry, filterEngine, pool, []redis.UniversalClient{rdb}, sharder, cfg.FraudStreamName)
	srv := httptest.NewServer(router)
	defer srv.Close()

	payload := map[string]any{
		"campaign_id": campaignID,
		"type":        "click",
		"payload":     map[string]string{"foo": "bar"},
	}
	body, _ := json.Marshal(payload)

	resp, err := http.Post(srv.URL+"/track", "application/json", bytes.NewBuffer(body))
	require.NoError(t, err)
	assert.Equal(t, http.StatusAccepted, resp.StatusCode)

	time.Sleep(1 * time.Second)

	assert.Eventually(t, func() bool {
		var clicks int64
		err = pool.QueryRow(ctx, "SELECT clicks_count FROM campaign_stats WHERE campaign_id = $1", campaignID).Scan(&clicks)
		return err == nil && clicks == 1
	}, 5*time.Second, 100*time.Millisecond, "Should have 1 click in campaign_stats")

	assert.Eventually(t, func() bool {
		var eventCount int
		err = pool.QueryRow(ctx, "SELECT count(*) FROM events WHERE campaign_id = $1", campaignID).Scan(&eventCount)
		return err == nil && eventCount == 1
	}, 5*time.Second, 100*time.Millisecond, "Should have 1 event in events table")
}

func TestE2EFlow_Protobuf(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := setupTestDB(t)
	defer cleanupDB()

	rdb, cleanupRedis := setupTestRedis(t)
	defer cleanupRedis()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queries := db.New(pool)
	cfg := &config.Config{
		EventBatchSize:     10,
		EventFlushMs:       100,
		StatsFlushMs:       100,
		MaxWorkers:         2,
		WriteTimeoutMs:     1000,
		FilterTimeoutMs:    1000,
		MaxRequestBodySize: 1024 * 1024,
		StreamMaxLen:       100000,
	}

	customerID := uuid.New()
	_, _ = pool.Exec(ctx, "INSERT INTO customers (id, name, balance) VALUES ($1, $2, $3)", customerID, "Proto Customer", 1_000_000_000)

	campaignID := uuid.New()
	_, _ = pool.Exec(ctx, "INSERT INTO campaigns (id, name, status, customer_id, budget_limit) VALUES ($1, $2, $3, $4, $5)", campaignID, "Proto Campaign", "ACTIVE", customerID, 100_000_000)

	registry := newTestRegistry(t, queries)
	_, _ = registry.Sync(ctx)

	store := ads.NewPostgresStore(queries, 1*time.Second)
	campaignRepo := ads.NewCampaignRepo(queries)
	unifiedFilter := ads.NewUnifiedFilter(
		[]redis.UniversalClient{rdb},
		ads.NewJumpHashSharder(1),
		registry,
		campaignRepo,
		1000,
		time.Minute,
		45*time.Second,
		24*time.Hour,
		100_000,
		10_000,
		"test-proto-stream",
		100000,
	)
	filterEngine := ads.NewFilterEngine(unifiedFilter)
	consumer := ads.NewStreamConsumer(store, rdb, "test-proto-stream", "test-proto-group", "test-c2", cfg.EventBatchSize, cfg.MaxWorkers, 100*time.Millisecond, 1*time.Second, 100*time.Millisecond, 5*time.Second, 5, 5*time.Minute, 1*time.Second)
	consumer.Start(ctx)
	defer consumer.Close()

	sharder := ads.NewJumpHashSharder(1)
	router := ads.NewRouter(cfg, registry, filterEngine, pool, []redis.UniversalClient{rdb}, sharder, cfg.FraudStreamName)
	srv := httptest.NewServer(router)
	defer srv.Close()

	pbEvt := &pb.AdEvent{
		CampaignId: []byte(campaignID.String()),
		EventType:  []byte("impression"),
		Metadata: &pb.EventMetadata{
			ClickId:    []byte("click_123"),
			UserId:     []byte("user_456"),
			DeviceType: []byte("mobile"),
			Os:         []byte("android"),
		},
	}
	body, _ := proto.Marshal(pbEvt)

	req, _ := http.NewRequest("POST", srv.URL+"/track", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusAccepted, resp.StatusCode)

	assert.Eventually(t, func() bool {
		var imps int64
		err = pool.QueryRow(ctx, "SELECT impressions_count FROM campaign_stats WHERE campaign_id = $1", campaignID).Scan(&imps)
		return err == nil && imps == 1
	}, 5*time.Second, 100*time.Millisecond, "Should have 1 impression in campaign_stats")
}
