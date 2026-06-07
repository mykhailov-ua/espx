package management

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/domain"
	"github.com/google/uuid"
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
		AdminAPIKey:       "test-secret",
		RateLimitPerMin:   100,
		RateLimitWindowMs: 60000,
	}

	svc := NewService(pool, []redis.UniversalClient{rdb}, nil, cfg)
	defer svc.Close()
	h := NewHandler(svc, cfg, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	ctx := context.Background()

	sw := ads.NewSettingsWatcher(rdb, cfg)
	assert.False(t, sw.Get().EmergencyBreaker)

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

	var dbVal string
	err := pool.QueryRow(ctx, "SELECT value FROM system_settings WHERE key = 'emergency_breaker'").Scan(&dbVal)
	require.NoError(t, err)
	assert.Equal(t, "true", dbVal)

	var auditCount int64
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM admin_audit_log WHERE action = 'EMERGENCY_BREAKER_TOGGLED'").Scan(&auditCount)
	require.NoError(t, err)
	assert.Equal(t, int64(1), auditCount)

	var outboxCount int64
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE event_type = 'UPDATE_SETTINGS'").Scan(&outboxCount)
	require.NoError(t, err)
	assert.Equal(t, int64(1), outboxCount)

	worker := NewOutboxWorker(svc)
	err = worker.ProcessOutbox(ctx)
	require.NoError(t, err)

	redisVal, err := rdb.HGet(ctx, "config:values", "emergency_breaker").Result()
	require.NoError(t, err)
	assert.Equal(t, "true", redisVal)

	watcherCtx, cancelWatcher := context.WithCancel(ctx)
	go sw.Start(watcherCtx, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return sw.Get().EmergencyBreaker
	}, 1*time.Second, 20*time.Millisecond)

	cancelWatcher()

	breakerFilter := ads.NewEmergencyBreakerFilter(sw)
	testEvt := &domain.Event{
		CampaignID: uuid.New(),
		Type:       "click",
		ClickID:    "click123",
		UserID:     "user1",
		IP:         "1.1.1.1",
	}

	err = breakerFilter.Check(ctx, testEvt)
	assert.ErrorIs(t, err, ads.ErrEmergencyBreakerActive)

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

	err = worker.ProcessOutbox(ctx)
	require.NoError(t, err)

	watcherCtx2, cancelWatcher2 := context.WithCancel(ctx)
	go sw.Start(watcherCtx2, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return !sw.Get().EmergencyBreaker
	}, 1*time.Second, 20*time.Millisecond)

	cancelWatcher2()

	err = breakerFilter.Check(ctx, testEvt)
	assert.NoError(t, err)
}
