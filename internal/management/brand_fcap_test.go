package management

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/domain"
	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBrandFrequencyCapping(t *testing.T) {
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

	ctx := context.Background()
	custID := uuid.New()
	err := svc.CreateCustomer(ctx, custID, "Brand Owner", 1_000_000_000, "USD")
	require.NoError(t, err)

	brandReq := map[string]any{
		"customer_id": custID.String(),
		"name":        "Nike Group",
	}
	brandReqBytes, _ := json.Marshal(brandReq)
	req, _ := http.NewRequest("POST", "/admin/brands", bytes.NewReader(brandReqBytes))
	req.Header.Set("X-Admin-API-Key", "test-secret")
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusCreated, resp.Code)

	var brandResp struct {
		ID string `json:"id"`
	}
	err = json.NewDecoder(resp.Body).Decode(&brandResp)
	require.NoError(t, err)
	brandID, err := uuid.Parse(brandResp.ID)
	require.NoError(t, err)

	reqList, _ := http.NewRequest("GET", "/admin/brands?customer_id="+custID.String(), nil)
	reqList.Header.Set("X-Admin-API-Key", "test-secret")
	respList := httptest.NewRecorder()
	mux.ServeHTTP(respList, reqList)
	require.Equal(t, http.StatusOK, respList.Code)

	var listResp []BrandDTO
	err = json.NewDecoder(respList.Body).Decode(&listResp)
	require.NoError(t, err)
	require.Len(t, listResp, 1)
	assert.Equal(t, brandResp.ID, listResp[0].ID)
	assert.Equal(t, custID.String(), listResp[0].CustomerID)
	assert.Equal(t, "Nike Group", listResp[0].Name)

	fcapReq := map[string]any{
		"freq_limit":  3,
		"freq_window": 3600,
	}
	fcapReqBytes, _ := json.Marshal(fcapReq)
	reqFcap, _ := http.NewRequest("POST", "/admin/brands/"+brandResp.ID+"/fcap", bytes.NewReader(fcapReqBytes))
	reqFcap.Header.Set("X-Admin-API-Key", "test-secret")
	respFcap := httptest.NewRecorder()
	mux.ServeHTTP(respFcap, reqFcap)
	require.Equal(t, http.StatusOK, respFcap.Code)

	var dbLimit, dbWindow int32
	err = pool.QueryRow(ctx, "SELECT freq_limit, freq_window FROM advertiser_brands WHERE id = $1", brandID).Scan(&dbLimit, &dbWindow)
	require.NoError(t, err)
	assert.Equal(t, int32(3), dbLimit)
	assert.Equal(t, int32(3600), dbWindow)

	var auditCount int64
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM admin_audit_log WHERE action = 'CONFIGURE_BRAND_FCAP' AND target_id = $1", brandID).Scan(&auditCount)
	require.NoError(t, err)
	assert.Equal(t, int64(1), auditCount)

	var outboxCount int64
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE event_type = 'CONFIGURE_BRAND_FCAP'").Scan(&outboxCount)
	require.NoError(t, err)
	assert.Equal(t, int64(1), outboxCount)

	campAReq := map[string]any{
		"customer_id":      custID.String(),
		"brand_id":         brandResp.ID,
		"name":             "Air Max Run",
		"budget_limit":     100.00,
		"daily_budget":     10.00,
		"freq_limit":       2,
		"freq_window":      3600,
		"target_countries": []string{"US"},
	}
	campAReqBytes, _ := json.Marshal(campAReq)
	reqA, _ := http.NewRequest("POST", "/admin/campaigns", bytes.NewReader(campAReqBytes))
	reqA.Header.Set("X-Admin-API-Key", "test-secret")
	respA := httptest.NewRecorder()
	mux.ServeHTTP(respA, reqA)
	require.Equal(t, http.StatusCreated, respA.Code)

	var campAResp struct {
		ID string `json:"id"`
	}
	err = json.NewDecoder(respA.Body).Decode(&campAResp)
	require.NoError(t, err)
	campAID, err := uuid.Parse(campAResp.ID)
	require.NoError(t, err)

	campBReq := map[string]any{
		"customer_id":      custID.String(),
		"brand_id":         brandResp.ID,
		"name":             "Air Max Walk",
		"budget_limit":     150.00,
		"daily_budget":     15.00,
		"freq_limit":       2,
		"freq_window":      3600,
		"target_countries": []string{"US"},
	}
	campBReqBytes, _ := json.Marshal(campBReq)
	reqB, _ := http.NewRequest("POST", "/admin/campaigns", bytes.NewReader(campBReqBytes))
	reqB.Header.Set("X-Admin-API-Key", "test-secret")
	respB := httptest.NewRecorder()
	mux.ServeHTTP(respB, reqB)
	require.Equal(t, http.StatusCreated, respB.Code)

	var campBResp struct {
		ID string `json:"id"`
	}
	err = json.NewDecoder(respB.Body).Decode(&campBResp)
	require.NoError(t, err)
	campBID, err := uuid.Parse(campBResp.ID)
	require.NoError(t, err)

	var brandFcapKeyA, brandFcapKeyB string
	var brandIDDbA, brandIDDbB uuid.UUID
	err = pool.QueryRow(ctx, "SELECT brand_id, brand_fcap_key FROM campaigns WHERE id = $1", campAID).Scan(&brandIDDbA, &brandFcapKeyA)
	require.NoError(t, err)
	err = pool.QueryRow(ctx, "SELECT brand_id, brand_fcap_key FROM campaigns WHERE id = $1", campBID).Scan(&brandIDDbB, &brandFcapKeyB)
	require.NoError(t, err)

	expectedFcapKey := "fcap:b:" + brandResp.ID
	assert.Equal(t, brandID, brandIDDbA)
	assert.Equal(t, brandID, brandIDDbB)
	assert.Equal(t, expectedFcapKey, brandFcapKeyA)
	assert.Equal(t, expectedFcapKey, brandFcapKeyB)

	queries := db.New(pool)
	registry := ads.NewRegistry(queries)
	registry.SetReplicaPath(filepath.Join(t.TempDir(), "campaigns_replica.json"))
	_, err = registry.Sync(ctx)
	require.NoError(t, err)

	sharder := ads.NewJumpHashSharder(1)
	filter := ads.NewUnifiedFilter(
		[]redis.UniversalClient{rdb},
		sharder,
		registry,
		nil,
		100,
		time.Minute,
		time.Hour,
		time.Hour,
		100_000,
		10_000,
		"events-stream",
		10000,
	)

	rdb.Set(ctx, "budget:campaign:"+campAID.String(), 1000000000, 0)
	rdb.Set(ctx, "budget:campaign:"+campBID.String(), 1000000000, 0)

	evtUser1A := &domain.Event{
		CampaignID: campAID,
		Type:       "click",
		ClickID:    "click_u1_a1",
		UserID:     "user_1",
		IP:         "1.1.1.1",
	}

	evtUser1B := &domain.Event{
		CampaignID: campBID,
		Type:       "click",
		ClickID:    "click_u1_b1",
		UserID:     "user_1",
		IP:         "1.1.1.1",
	}

	evtUser1ASecond := &domain.Event{
		CampaignID: campAID,
		Type:       "click",
		ClickID:    "click_u1_a2",
		UserID:     "user_1",
		IP:         "1.1.1.1",
	}

	err = filter.Check(ctx, evtUser1A)
	assert.NoError(t, err)

	err = filter.Check(ctx, evtUser1B)
	assert.NoError(t, err)

	err = filter.Check(ctx, evtUser1ASecond)
	assert.ErrorIs(t, err, ads.ErrFreqLimitExceeded)
}
