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

// TestManagementAPI_System guards settings and blacklist write cycles propagate to Redis.
func TestManagementAPI_System(t *testing.T) {
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

	t.Run("SettingsCycle", func(t *testing.T) {
		settings := map[string]string{
			"rate_limit_per_min": "100",
			"click_amount":       "0.05",
		}
		body, _ := json.Marshal(settings)
		req, _ := http.NewRequest("POST", "/admin/settings", bytes.NewReader(body))
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusNoContent, resp.Code)

		reqGet, _ := http.NewRequest("GET", "/admin/settings", nil)
		reqGet.Header.Set("X-Admin-API-Key", "test-secret")
		respGet := httptest.NewRecorder()
		mux.ServeHTTP(respGet, reqGet)
		assert.Equal(t, http.StatusOK, respGet.Code)

		var res map[string]string
		err := json.NewDecoder(respGet.Body).Decode(&res)
		require.NoError(t, err)
		assert.Equal(t, "100", res["rate_limit_per_min"])
		assert.Equal(t, "0.05", res["click_amount"])

		assert.Eventually(t, func() bool {
			val, err := rdb.HGet(context.Background(), "config:values", "rate_limit_per_min").Result()
			return err == nil && val == "100"
		}, 2*time.Second, 20*time.Millisecond)
	})

	t.Run("BlacklistCycle", func(t *testing.T) {
		reqBody := map[string]string{
			"ip":     "192.168.1.50",
			"source": "fraud",
		}
		body, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", "/admin/blacklist", bytes.NewReader(body))
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusCreated, resp.Code)

		assert.Eventually(t, func() bool {
			isMember, err := rdb.SIsMember(context.Background(), "blacklist:fraud", "192.168.1.50").Result()
			return err == nil && isMember
		}, 2*time.Second, 20*time.Millisecond)

		reqList, _ := http.NewRequest("GET", "/admin/blacklist", nil)
		reqList.Header.Set("X-Admin-API-Key", "test-secret")
		respList := httptest.NewRecorder()
		mux.ServeHTTP(respList, reqList)
		assert.Equal(t, http.StatusOK, respList.Code)
		assert.NotEmpty(t, respList.Header().Get("X-Total-Count"))

		var bl []BlacklistDTO
		err := json.NewDecoder(respList.Body).Decode(&bl)
		require.NoError(t, err)
		require.NotEmpty(t, bl)
		assert.Equal(t, "192.168.1.50", bl[0].IP)
		assert.Equal(t, "fraud", bl[0].Reason)

		err = svc.SyncSystemState(context.Background())
		require.NoError(t, err)

		reqDel, _ := http.NewRequest("DELETE", "/admin/blacklist", bytes.NewReader(body))
		reqDel.Header.Set("X-Admin-API-Key", "test-secret")
		respDel := httptest.NewRecorder()
		mux.ServeHTTP(respDel, reqDel)
		assert.Equal(t, http.StatusNoContent, respDel.Code)

		assert.Eventually(t, func() bool {
			isMember, err := rdb.SIsMember(context.Background(), "blacklist:fraud", "192.168.1.50").Result()
			return err == nil && !isMember
		}, 2*time.Second, 20*time.Millisecond)
	})
}
