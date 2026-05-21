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
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagementAPI_CampaignPacing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		AdminAPIKey:           "test-secret-pacing",
		CampaignUpdateChannel: "test:campaign-updates-pacing",
	}

	svc := NewService(pool, []redis.UniversalClient{rdb}, nil, cfg)
	defer svc.Close()
	h := NewHandler(svc, cfg, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	// Create test customer & campaign
	custID := uuid.New()
	err := svc.CreateCustomer(context.Background(), custID, "Advertiser Pacing", decimal.NewFromFloat(500.00), "USD")
	require.NoError(t, err)

	campID, err := svc.CreateCampaign(
		context.Background(),
		custID,
		nil,
		"Spring Sale Pacing",
		decimal.NewFromFloat(100.00),
		db.PacingModeTypeEVEN,
		decimal.NewFromFloat(10.00),
		"UTC",
		5,
		3600,
		[]string{"US", "GB"},
		"idemp-camp-pacing-1",
	)
	require.NoError(t, err)

	t.Run("InvalidPacingMode_BadRequest", func(t *testing.T) {
		body, _ := json.Marshal(map[string]string{"pacing_mode": "INVALID"})
		req, _ := http.NewRequest("POST", "/admin/campaigns/"+campID.String()+"/pacing", bytes.NewReader(body))
		req.Header.Set("X-Admin-API-Key", "test-secret-pacing")
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("CampaignIsolation_Forbidden", func(t *testing.T) {
		otherCustID := uuid.New()
		body, _ := json.Marshal(map[string]string{"pacing_mode": "ASAP"})
		req, _ := http.NewRequest("POST", "/admin/campaigns/"+campID.String()+"/pacing", bytes.NewReader(body))
		req.Header.Set("X-Admin-API-Key", "test-secret-pacing")

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

	t.Run("UpdatePacing_Success", func(t *testing.T) {
		body, _ := json.Marshal(map[string]string{"pacing_mode": "ASAP"})
		req, _ := http.NewRequest("POST", "/admin/campaigns/"+campID.String()+"/pacing", bytes.NewReader(body))
		req.Header.Set("X-Admin-API-Key", "test-secret-pacing")

		user := AuthenticatedUser{
			UserID:     uuid.New(),
			Role:       "C",
			CustomerID: custID,
		}
		ctx := context.WithValue(req.Context(), UserContextKey, user)

		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req.WithContext(ctx))

		assert.Equal(t, http.StatusOK, resp.Code)

		var camp CampaignDTO
		err := json.NewDecoder(resp.Body).Decode(&camp)
		require.NoError(t, err)
		assert.Equal(t, "ASAP", camp.PacingMode)

		// Verify persisted state in Postgres
		var currentPacing string
		err = pool.QueryRow(context.Background(), "SELECT pacing_mode FROM campaigns WHERE id = $1", campID).Scan(&currentPacing)
		require.NoError(t, err)
		assert.Equal(t, "ASAP", currentPacing)

		// Verify audit log entry
		var auditCount int
		err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM admin_audit_log WHERE action = 'UPDATE_CAMPAIGN_PACING' AND target_id = $1", campID).Scan(&auditCount)
		require.NoError(t, err)
		assert.Equal(t, 1, auditCount)

		// Verify outbox worker processes the update to Redis & PubSub
		// Wait for OutboxWorker background poll
		assert.Eventually(t, func() bool {
			val, rdbErr := rdb.HGet(context.Background(), "campaign:settings:"+campID.String(), "pacing_mode").Result()
			return rdbErr == nil && val == "ASAP"
		}, 3*time.Second, 50*time.Millisecond)
	})
}
