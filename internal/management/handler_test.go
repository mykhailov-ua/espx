package management

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"espx/internal/config"
	"espx/internal/database"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagementAPI_Hardening(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		AdminAPIKey: "test-secret",
	}

	svc := NewService(pool, []redis.UniversalClient{rdb}, nil, cfg)
	defer svc.Close()
	h := NewHandler(svc, cfg, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	t.Run("UpdateSettings_Unauthorized", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "/admin/settings", bytes.NewBufferString("{}"))
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusUnauthorized, resp.Code)
	})

	t.Run("UpdateSettings_Success", func(t *testing.T) {
		settings := map[string]string{"rate_limit_per_min": "500"}
		body, _ := json.Marshal(settings)
		req, _ := http.NewRequest("POST", "/admin/settings", bytes.NewBuffer(body))
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusNoContent, resp.Code)

		assert.Eventually(t, func() bool {
			v, _ := rdb.Get(context.Background(), "config:version").Int64()
			return v == int64(1)
		}, 2*time.Second, 20*time.Millisecond)
	})

	t.Run("Blacklist_Success", func(t *testing.T) {
		payload := map[string]string{"ip": "9.9.9.9", "source": "manual"}
		body, _ := json.Marshal(payload)
		req, _ := http.NewRequest("POST", "/admin/blacklist", bytes.NewBuffer(body))
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusCreated, resp.Code)

		isMember, _ := rdb.SIsMember(context.Background(), "blacklist:manual", "9.9.9.9").Result()
		assert.True(t, isMember)
	})

	t.Run("ListAudit", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/admin/audit?limit=10", nil)
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusOK, resp.Code)

		var logs []any
		err := json.NewDecoder(resp.Body).Decode(&logs)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(logs), 2)
	})

	t.Run("RateLimit", func(t *testing.T) {

		for i := 0; i < 60; i++ {
			req, _ := http.NewRequest("GET", "/admin/audit", nil)
			req.Header.Set("X-Admin-API-Key", "test-secret")
			resp := httptest.NewRecorder()
			mux.ServeHTTP(resp, req)
			if resp.Code == http.StatusTooManyRequests {
				return
			}
		}
		t.Errorf("Rate limiter did not trigger after 60 requests")
	})
}
