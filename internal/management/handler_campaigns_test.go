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
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/database"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagementAPI_Campaigns(t *testing.T) {
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

	// Create test customer & campaign
	custID := uuid.New()
	err := svc.CreateCustomer(context.Background(), custID, "Advertiser", 500_000_000, "USD")
	require.NoError(t, err)

	campID, err := svc.CreateCampaign(
		context.Background(),
		custID,
		nil,
		"Spring Sale",
		100_000_000,
		db.PacingModeTypeEVEN,
		10_000_000,
		"UTC",
		5,
		3600,
		[]string{"US", "GB"},
		"idemp-camp-1",
	)
	require.NoError(t, err)

	t.Run("ListCampaigns", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/admin/campaigns?status=ACTIVE&customer_id="+custID.String(), nil)
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)
		assert.NotEmpty(t, resp.Header().Get("X-Total-Count"))

		var campaigns []CampaignDTO
		err := json.NewDecoder(resp.Body).Decode(&campaigns)
		require.NoError(t, err)
		require.NotEmpty(t, campaigns)

		var found *CampaignDTO
		for _, c := range campaigns {
			if c.ID == campID.String() {
				found = &c
				break
			}
		}
		require.NotNil(t, found)
		assert.Equal(t, "Spring Sale", found.Name)
		assert.Equal(t, "100.00", found.BudgetLimit)
		assert.Equal(t, "0.00", found.CurrentSpend)
		assert.Equal(t, []string{"US", "GB"}, found.TargetCountries)
	})

	t.Run("GetCampaignByID", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/admin/campaigns/"+campID.String(), nil)
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)

		var camp CampaignDTO
		err := json.NewDecoder(resp.Body).Decode(&camp)
		require.NoError(t, err)
		assert.Equal(t, campID.String(), camp.ID)
		assert.Equal(t, "100.00", camp.BudgetLimit)
		assert.NotNil(t, camp.TargetCountries) // Ensure non-null slice
	})

	t.Run("GetCampaignHistory", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/admin/campaigns/"+campID.String()+"/history", nil)
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)

		var history []StatusHistoryDTO
		err := json.NewDecoder(resp.Body).Decode(&history)
		require.NoError(t, err)
		require.NotEmpty(t, history)
		assert.Equal(t, "ACTIVE", history[0].NewStatus)
	})

	t.Run("CampaignIsolation_Forbidden", func(t *testing.T) {
		otherCustID := uuid.New()

		req, _ := http.NewRequest("GET", "/admin/campaigns/"+campID.String(), nil)
		req.Header.Set("X-Admin-API-Key", "test-secret")

		user := AuthenticatedUser{
			UserID:     uuid.New(),
			Role:       "C",
			CustomerID: otherCustID,
		}
		ctx := context.WithValue(req.Context(), UserContextKey, user)

		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req.WithContext(ctx))

		assert.Equal(t, http.StatusForbidden, resp.Code)
	})

	t.Run("CancelCampaign_Accepted", func(t *testing.T) {
		body, _ := json.Marshal(map[string]string{"reason": "User requested cancellation"})
		req, _ := http.NewRequest("DELETE", "/admin/campaigns/"+campID.String(), bytes.NewReader(body))
		req.Header.Set("X-Admin-API-Key", "test-secret")

		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusAccepted, resp.Code)

		assert.Eventually(t, func() bool {
			var status string
			_ = pool.QueryRow(context.Background(), "SELECT status FROM campaigns WHERE id = $1", campID).Scan(&status)
			return status == "DELETED"
		}, 2*time.Second, 20*time.Millisecond)
	})
}
