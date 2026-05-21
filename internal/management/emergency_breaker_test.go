package management

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/database"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmergencyCircuitBreaker(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		AdminAPIKey: "test-secret",
		RateLimitPerMin: 100,
		RateLimitWindowMs: 60000,
	}

	svc := NewService(pool, []redis.UniversalClient{rdb}, nil, cfg)
	defer svc.Close()
	h := NewHandler(svc, cfg, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	ctx := context.Background()

	// 1. Initial State: settings watcher has breaker as false
	sw := ads.NewSettingsWatcher(rdb, cfg)
	assert.False(t, sw.Get().EmergencyBreaker)

	// 2. Toggle Breaker ON via REST API
	reqBody := map[string]any{
		"active": true,
		"reason": "high CPU fraud spike",
	}
	reqBytes, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/admin/system/breaker", bytes.NewReader(reqBytes))
	req.Header.Set("X-Admin-API-Key", "test-secret")
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)

	// 3. Verify Database Updates
	var dbVal string
	err := pool.QueryRow(ctx, "SELECT value FROM system_settings WHERE key = 'emergency_breaker'").Scan(&dbVal)
	require.NoError(t, err)
	assert.Equal(t, "true", dbVal)

	// 4. Verify Audit Logs
	var auditCount int64
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM admin_audit_log WHERE action = 'EMERGENCY_BREAKER_TOGGLED'").Scan(&auditCount)
	require.NoError(t, err)
	assert.Equal(t, int64(1), auditCount)

	// 5. Verify Outbox Events
	var outboxCount int64
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE event_type = 'UPDATE_SETTINGS'").Scan(&outboxCount)
	require.NoError(t, err)
	assert.Equal(t, int64(1), outboxCount)

	// 6. Process Outbox using our Worker to sync settings to Redis
	worker := NewOutboxWorker(svc)
	err = worker.ProcessOutbox(ctx)
	require.NoError(t, err)

	// Verify Redis holds the value
	redisVal, err := rdb.HGet(ctx, "config:values", "emergency_breaker").Result()
	require.NoError(t, err)
	assert.Equal(t, "true", redisVal)

	// 7. Settings Watcher picks it up
	// Triggers dynamic configuration refresh in settings watcher
	// Since sw.Start blocks, let's test it by executing the internal sync logic (we can just trigger a sleep or check sw.Get() after dynamic update)
	// But let's look: since we can't call unexported sw.sync(ctx) directly from another package because settings.go is in package ads,
	// and we are in package management.
	// However, we can run a background go routine for sw.Start and cancel its context!
	watcherCtx, cancelWatcher := context.WithCancel(ctx)
	go sw.Start(watcherCtx, 10*time.Millisecond)

	// Wait up to 500ms for settings watcher to sync the update from Redis
	require.Eventually(t, func() bool {
		return sw.Get().EmergencyBreaker
	}, 1*time.Second, 20*time.Millisecond)

	cancelWatcher()

	// 8. Test Ingestion Layer Filter Behavior
	breakerFilter := ads.NewEmergencyBreakerFilter(sw)
	testEvt := &domain.Event{
		CampaignID: uuid.New(),
		Type:       "click",
		ClickID:    "click123",
		UserID:     "user1",
		IP:         "1.1.1.1",
	}

	// Active breaker blocks the request
	err = breakerFilter.Check(ctx, testEvt)
	assert.ErrorIs(t, err, ads.ErrEmergencyBreakerActive)

	// 9. Toggle Breaker OFF via REST API
	reqBodyOff := map[string]any{
		"active": false,
		"reason": "mitigation completed",
	}
	reqBytesOff, _ := json.Marshal(reqBodyOff)
	reqOff, _ := http.NewRequest("POST", "/admin/system/breaker", bytes.NewReader(reqBytesOff))
	reqOff.Header.Set("X-Admin-API-Key", "test-secret")
	respOff := httptest.NewRecorder()
	mux.ServeHTTP(respOff, reqOff)
	require.Equal(t, http.StatusOK, respOff.Code)

	// Process outbox to update Redis
	err = worker.ProcessOutbox(ctx)
	require.NoError(t, err)

	// Restart SettingsWatcher with new context to fetch the updated state
	watcherCtx2, cancelWatcher2 := context.WithCancel(ctx)
	go sw.Start(watcherCtx2, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return !sw.Get().EmergencyBreaker
	}, 1*time.Second, 20*time.Millisecond)

	cancelWatcher2()

	// Inactive breaker allows the request
	err = breakerFilter.Check(ctx, testEvt)
	assert.NoError(t, err)
}
