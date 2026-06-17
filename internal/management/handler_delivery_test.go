package management

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"espx/internal/ads/db"
	"espx/internal/auth"
	"espx/internal/config"
	"espx/internal/database"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestManagementAPI_DeliveryRoutes guards delivery HTTP routes for templates, pause or resume, schedule, and creatives.
func TestManagementAPI_DeliveryRoutes(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		AdminAPIKey:           "test-secret",
		CampaignUpdateChannel: "test:delivery-routes",
	}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	h := NewHandler(svc, cfg, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	ctx := context.Background()
	custID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, custID, "Delivery Customer", 800_000_000, "USD"))

	brandID, err := svc.CreateBrand(ctx, custID, "Delivery Brand")
	require.NoError(t, err)

	var templateID uuid.UUID
	t.Run("CreateCampaignTemplate", func(t *testing.T) {
		body, _ := json.Marshal(map[string]any{
			"customer_id":      custID,
			"name":             "Tpl HTTP",
			"budget_limit":     50.0,
			"pacing_mode":      "EVEN",
			"daily_budget":     10.0,
			"timezone":         "UTC",
			"freq_window":      86400,
			"target_countries": []string{},
			"daypart_hours":    []int16{9, 10},
		})
		req, _ := http.NewRequest("POST", "/admin/campaign-templates", bytes.NewReader(body))
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)
		if resp.Code != http.StatusCreated {
			t.Fatalf("CreateCampaignTemplate: status=%d body=%s", resp.Code, resp.Body.String())
		}

		var res map[string]string
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&res))
		templateID, err = uuid.Parse(res["id"])
		require.NoError(t, err)
	})

	t.Run("ListCampaignTemplates", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/admin/campaign-templates?customer_id="+custID.String(), nil)
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)
		require.Equal(t, http.StatusOK, resp.Code)
		assert.NotEmpty(t, resp.Header().Get("X-Total-Count"))
	})

	var campID uuid.UUID
	t.Run("CreateCampaignFromTemplate", func(t *testing.T) {
		body, _ := json.Marshal(map[string]any{
			"customer_id": custID,
			"name":        "From Template HTTP",
		})
		req, _ := http.NewRequest("POST", "/admin/campaign-templates/"+templateID.String()+"/instantiate", bytes.NewReader(body))
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)
		require.Equal(t, http.StatusCreated, resp.Code)

		var res map[string]string
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&res))
		campID, err = uuid.Parse(res["id"])
		require.NoError(t, err)
	})

	t.Run("SaveCampaignAsTemplate", func(t *testing.T) {
		body, _ := json.Marshal(map[string]string{"name": "Saved From Camp"})
		req, _ := http.NewRequest("POST", "/admin/campaigns/"+campID.String()+"/save-as-template", bytes.NewReader(body))
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)
		require.Equal(t, http.StatusCreated, resp.Code)
	})

	t.Run("PauseAndResume", func(t *testing.T) {
		body, _ := json.Marshal(map[string]string{"reason": "manual pause"})
		req, _ := http.NewRequest("POST", "/admin/campaigns/"+campID.String()+"/pause", bytes.NewReader(body))
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)
		require.Equal(t, http.StatusAccepted, resp.Code)

		var status string
		require.NoError(t, pool.QueryRow(ctx, `SELECT status::TEXT FROM campaigns WHERE id = $1`, campID).Scan(&status))
		assert.Equal(t, "PAUSED", status)

		body, _ = json.Marshal(map[string]string{"reason": "manual resume"})
		req, _ = http.NewRequest("POST", "/admin/campaigns/"+campID.String()+"/resume", bytes.NewReader(body))
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp = httptest.NewRecorder()
		mux.ServeHTTP(resp, req)
		require.Equal(t, http.StatusAccepted, resp.Code)

		require.NoError(t, pool.QueryRow(ctx, `SELECT status::TEXT FROM campaigns WHERE id = $1`, campID).Scan(&status))
		assert.Equal(t, "ACTIVE", status)
	})

	t.Run("UpdateCampaignSchedule", func(t *testing.T) {
		start := time.Now().Add(24 * time.Hour)
		end := time.Now().Add(72 * time.Hour)
		body, _ := json.Marshal(map[string]any{
			"start_at":      start,
			"end_at":        end,
			"daypart_hours": []int16{8, 9},
		})
		req, _ := http.NewRequest("POST", "/admin/campaigns/"+campID.String()+"/schedule", bytes.NewReader(body))
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)
		require.Equal(t, http.StatusNoContent, resp.Code)

		camp, err := svc.GetCampaign(ctx, campID)
		require.NoError(t, err)
		assert.Equal(t, db.CampaignStatusTypePAUSED, camp.Status)
	})

	var creativeID uuid.UUID
	t.Run("BrandCreativesCRUD", func(t *testing.T) {
		createBody, _ := json.Marshal(map[string]any{
			"name":        "Creative A",
			"landing_url": "https://example.com/a",
			"weight":      100,
			"status":      "ACTIVE",
		})
		req, _ := http.NewRequest("POST", "/admin/brands/"+brandID.String()+"/creatives", bytes.NewReader(createBody))
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)
		require.Equal(t, http.StatusCreated, resp.Code)

		var created map[string]string
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&created))
		creativeID, err = uuid.Parse(created["id"])
		require.NoError(t, err)

		req, _ = http.NewRequest("GET", "/admin/brands/"+brandID.String()+"/creatives", nil)
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp = httptest.NewRecorder()
		mux.ServeHTTP(resp, req)
		require.Equal(t, http.StatusOK, resp.Code)

		updateBody, _ := json.Marshal(map[string]any{
			"name":        "Creative A v2",
			"landing_url": "https://example.com/a2",
			"weight":      200,
			"status":      "ACTIVE",
		})
		req, _ = http.NewRequest("PUT", "/admin/brands/"+brandID.String()+"/creatives/"+creativeID.String(), bytes.NewReader(updateBody))
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp = httptest.NewRecorder()
		mux.ServeHTTP(resp, req)
		require.Equal(t, http.StatusNoContent, resp.Code)

		req, _ = http.NewRequest("DELETE", "/admin/brands/"+brandID.String()+"/creatives/"+creativeID.String(), nil)
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp = httptest.NewRecorder()
		mux.ServeHTTP(resp, req)
		require.Equal(t, http.StatusNoContent, resp.Code)
	})
}

// TestManagementAPI_RoleUserForbiddenSettings guards role user cannot read or write system settings.
func TestManagementAPI_RoleUserForbiddenSettings(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		TokenSymmetricKey: "01234567890123456789012345678901",
	}
	tokenMaker, err := auth.NewPasetoMaker(string(cfg.TokenSymmetricKey))
	require.NoError(t, err)

	authMdl := NewAuthMiddleware(tokenMaker, rdb, cfg)
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	h := NewHandler(svc, cfg, authMdl)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	customerID := uuid.New()
	token, err := tokenMaker.CreateToken(uuid.New(), uuid.New(), "user", customerID, time.Hour)
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]string{"rate_limit_per_min": "999"})
	req, _ := http.NewRequest("POST", "/admin/settings", bytes.NewReader(body))
	req.AddCookie(&http.Cookie{Name: "accessToken", Value: token})
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)

	assert.Equal(t, http.StatusForbidden, resp.Code)
	assert.Contains(t, resp.Body.String(), "FORBIDDEN")

	reqGet, _ := http.NewRequest("GET", "/admin/settings", nil)
	reqGet.AddCookie(&http.Cookie{Name: "accessToken", Value: token})
	respGet := httptest.NewRecorder()
	mux.ServeHTTP(respGet, reqGet)
	assert.Equal(t, http.StatusForbidden, respGet.Code)
}

// TestManagementAPI_RoleUserForbiddenBlacklist guards role user cannot modify IP blacklist.
func TestManagementAPI_RoleUserForbiddenBlacklist(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{TokenSymmetricKey: "01234567890123456789012345678901"}
	tokenMaker, err := auth.NewPasetoMaker(string(cfg.TokenSymmetricKey))
	require.NoError(t, err)

	authMdl := NewAuthMiddleware(tokenMaker, rdb, cfg)
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	h := NewHandler(svc, cfg, authMdl)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	token, err := tokenMaker.CreateToken(uuid.New(), uuid.New(), "user", uuid.New(), time.Hour)
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]string{"ip": "1.2.3.4", "source": "manual"})
	req, _ := http.NewRequest("POST", "/admin/blacklist", bytes.NewReader(body))
	req.AddCookie(&http.Cookie{Name: "accessToken", Value: token})
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	assert.Equal(t, http.StatusForbidden, resp.Code)
}

// TestManagementAPI_RoleUserForbiddenEmergencyBreaker guards role user cannot toggle the emergency breaker.
func TestManagementAPI_RoleUserForbiddenEmergencyBreaker(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{TokenSymmetricKey: "01234567890123456789012345678901"}
	tokenMaker, err := auth.NewPasetoMaker(string(cfg.TokenSymmetricKey))
	require.NoError(t, err)

	authMdl := NewAuthMiddleware(tokenMaker, rdb, cfg)
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	h := NewHandler(svc, cfg, authMdl)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	token, err := tokenMaker.CreateToken(uuid.New(), uuid.New(), "user", uuid.New(), time.Hour)
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]any{"active": true, "reason": "test"})
	req, _ := http.NewRequest("POST", "/admin/system/breaker", bytes.NewReader(body))
	req.AddCookie(&http.Cookie{Name: "accessToken", Value: token})
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	assert.Equal(t, http.StatusForbidden, resp.Code)
}
