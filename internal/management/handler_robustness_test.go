package management

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/database"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestManagementAPI_Robustness(t *testing.T) {
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
	h := NewHandler(svc, cfg, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	t.Run("InvalidInput_MalformedJSON", func(t *testing.T) {
		endpoints := []struct {
			method string
			path   string
		}{
			{"POST", "/admin/customers"},
			{"POST", "/admin/customers/00000000-0000-0000-0000-000000000000/topup"},
			{"POST", "/admin/campaigns"},
			{"POST", "/admin/settings"},
			{"POST", "/admin/blacklist"},
			{"DELETE", "/admin/blacklist"},
		}

		for _, tc := range endpoints {
			req, _ := http.NewRequest(tc.method, tc.path, bytes.NewBufferString("{malformed-json"))
			req.Header.Set("X-Admin-API-Key", "test-secret")
			resp := httptest.NewRecorder()
			mux.ServeHTTP(resp, req)
			assert.Equal(t, http.StatusBadRequest, resp.Code, "expected 400 for %s %s", tc.method, tc.path)
		}
	})

	t.Run("InvalidInput_InvalidUUID_URL", func(t *testing.T) {
		paths := []struct {
			method string
			path   string
		}{
			{"GET", "/admin/customers/invalid-uuid"},
			{"GET", "/admin/customers/invalid-uuid/ledger"},
			{"GET", "/admin/campaigns/invalid-uuid"},
			{"GET", "/admin/campaigns/invalid-uuid/history"},
			{"DELETE", "/admin/campaigns/invalid-uuid"},
		}

		for _, tc := range paths {
			req, _ := http.NewRequest(tc.method, tc.path, nil)
			req.Header.Set("X-Admin-API-Key", "test-secret")
			resp := httptest.NewRecorder()
			mux.ServeHTTP(resp, req)
			assert.Equal(t, http.StatusBadRequest, resp.Code, "expected 400 for %s %s", tc.method, tc.path)
		}
	})

	t.Run("DBFailure_Simulation", func(t *testing.T) {
		// Close the DB connection pool to simulate DB outage
		badPool, cleanupBadDB := database.SetupTestDB(t)
		cleanupBadDB() // immediately close it

		badSvc := NewService(badPool, []redis.UniversalClient{rdb}, nil, cfg)
		badH := NewHandler(badSvc, cfg, nil)
		badMux := http.NewServeMux()
		badH.RegisterRoutes(badMux)

		paths := []string{
			"/admin/customers",
			"/admin/campaigns",
			"/admin/blacklist",
			"/admin/settings",
			"/admin/audit",
		}

		for _, path := range paths {
			req, _ := http.NewRequest("GET", path, nil)
			req.Header.Set("X-Admin-API-Key", "test-secret")
			resp := httptest.NewRecorder()
			badMux.ServeHTTP(resp, req)
			assert.Equal(t, http.StatusInternalServerError, resp.Code, "expected 500 for DB failure GET %s", path)
		}
	})

	t.Run("BackgroundSync_GoroutineLifecycle", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			svc.RunSystemStateSyncer(ctx)
		}()

		// Allow initial sync to complete
		time.Sleep(50 * time.Millisecond)

		// Cancel context to test clean shutdown without deadlocks
		cancel()

		// Wait for goroutine to exit with timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(1 * time.Second):
			t.Fatal("RunSystemStateSyncer goroutine did not exit after context cancellation (potential deadlock)")
		}
	})
}
