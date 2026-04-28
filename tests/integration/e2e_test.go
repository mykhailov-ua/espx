package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/campaign"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/database"
	"github.com/mykhailov-ua/ad-event-processor/internal/database/db"
	"github.com/mykhailov-ua/ad-event-processor/internal/event"
	"github.com/mykhailov-ua/ad-event-processor/internal/server"
	"github.com/mykhailov-ua/ad-event-processor/internal/stats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestE2EFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queries := db.New(pool)
	cfg := &config.Config{
		EventBatchSize: 10,
		EventFlushMs:   100,
		StatsFlushMs:   100,
		MaxWorkers:     2,
		WriteTimeoutMs: 1000,
	}

	partManager := database.NewPartitionManager(pool, 7, 2)
	err := partManager.Run(ctx)
	require.NoError(t, err)

	campaignID := uuid.New()
	_, err = pool.Exec(ctx, "INSERT INTO campaigns (id, name, status) VALUES ($1, $2, $3)", campaignID, "E2E Campaign", "active")
	require.NoError(t, err)

	registry := campaign.NewRegistry(queries)
	_, _ = registry.Sync(ctx)

	eventProc := event.NewProcessor(pool, cfg.EventBatchSize, cfg.MaxWorkers, 100*time.Millisecond, 1*time.Second)
	eventProc.Start(ctx)
	defer eventProc.Close()

	statsAgg := stats.NewAggregator(queries, 100*time.Millisecond, 1*time.Second, cfg.MaxWorkers)
	statsAgg.Start(ctx)

	router := server.NewRouter(cfg, registry, eventProc, statsAgg)
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

	time.Sleep(500 * time.Millisecond)

	var clicks int64
	err = pool.QueryRow(ctx, "SELECT clicks_count FROM campaign_stats WHERE campaign_id = $1", campaignID).Scan(&clicks)
	require.NoError(t, err)
	assert.Equal(t, int64(1), clicks)

	var eventCount int
	err = pool.QueryRow(ctx, "SELECT count(*) FROM events WHERE campaign_id = $1", campaignID).Scan(&eventCount)
	require.NoError(t, err)
	assert.Equal(t, 1, eventCount)
}
