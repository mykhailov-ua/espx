package management

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"espx/internal/ads/db"
	"espx/internal/config"
	"espx/pkg/httpresponse"
	"github.com/google/uuid"
)

// Handler serves the admin HTTP API with auth, rate limiting, and permission checks.
type Handler struct {
	svc            *Service
	cfg            *config.Config
	ipLimiter      *ipRateLimiter
	authMiddleware *AuthMiddleware
}

// NewHandler constructs the admin HTTP handler with per-IP rate limits from config.
func NewHandler(svc *Service, cfg *config.Config, authMiddleware *AuthMiddleware) *Handler {
	rps := 10.0
	burst := 50
	if cfg != nil {
		rps = cfg.Management.RateLimitRPS
		burst = cfg.Management.RateLimitBurst
	}
	return &Handler{
		svc:            svc,
		cfg:            cfg,
		ipLimiter:      newIPRateLimiter(rps, burst),
		authMiddleware: authMiddleware,
	}
}

// RegisterRoutes mounts all admin endpoints on the provided mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /admin/customers", h.limit(h.perm(h.createCustomer, PermCustomersWrite)))
	mux.HandleFunc("POST /admin/customers/{id}/topup", h.limit(h.perm(h.topUpBalance, PermCustomersWrite)))
	mux.HandleFunc("POST /admin/campaigns", h.limit(h.perm(h.createCampaign, PermCampaignsWrite)))
	mux.HandleFunc("POST /admin/brands", h.limit(h.perm(h.createBrand, PermBrandsWrite)))
	mux.HandleFunc("GET /admin/brands", h.limit(h.perm(h.listBrands, PermBrandsRead)))
	mux.HandleFunc("POST /admin/brands/{id}/fcap", h.limit(h.perm(h.configureBrandFcap, PermBrandsWrite)))
	mux.HandleFunc("DELETE /admin/campaigns/{id}", h.limit(h.perm(h.cancelCampaign, PermCampaignsWrite)))
	mux.HandleFunc("POST /admin/campaigns/{id}/pacing", h.limit(h.perm(h.updateCampaignPacing, PermCampaignsWrite)))

	mux.HandleFunc("POST /admin/settings", h.limit(h.perm(h.updateSettings, PermSettingsWrite)))
	mux.HandleFunc("POST /admin/blacklist", h.limit(h.perm(h.blockIP, PermBlacklistWrite)))
	mux.HandleFunc("DELETE /admin/blacklist", h.limit(h.perm(h.unblockIP, PermBlacklistWrite)))
	mux.HandleFunc("GET /admin/audit", h.limit(h.perm(h.listAudit, PermAuditRead)))
	mux.HandleFunc("POST /admin/system/breaker", h.limit(h.perm(h.toggleEmergencyBreaker, PermSettingsWrite)))

	mux.HandleFunc("GET /admin/customers", h.limit(h.perm(h.listCustomers, PermCustomersRead)))
	mux.HandleFunc("GET /admin/customers/{id}", h.limit(h.perm(h.getCustomer, PermCustomersRead)))
	mux.HandleFunc("GET /admin/customers/{id}/ledger", h.limit(h.perm(h.getCustomerLedger, PermCustomersRead)))

	mux.HandleFunc("GET /admin/campaigns", h.limit(h.perm(h.listCampaigns, PermCampaignsRead)))
	mux.HandleFunc("GET /admin/campaigns/{id}", h.limit(h.perm(h.getCampaign, PermCampaignsRead)))
	mux.HandleFunc("GET /admin/campaigns/{id}/history", h.limit(h.perm(h.getCampaignHistory, PermCampaignsRead)))

	mux.HandleFunc("GET /admin/blacklist", h.limit(h.perm(h.listBlacklist, PermBlacklistRead)))
	mux.HandleFunc("GET /admin/settings", h.limit(h.perm(h.getSettings, PermSettingsRead)))
	h.registerDeliveryRoutes(mux)
}

// limit wraps handlers with a per-client IP token bucket.
func (h *Handler) limit(next http.HandlerFunc) http.HandlerFunc {
	return h.limitByIP(next)
}

// perm wraps handlers with permission-based authentication.
func (h *Handler) perm(next http.HandlerFunc, permission string) http.HandlerFunc {
	if h.authMiddleware != nil {
		return h.authMiddleware.RequirePermission(permission)(next)
	}
	return h.authFallback(next)
}

// authFallback allows integration tests to call admin routes with only the shared API key.
func (h *Handler) authFallback(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-Admin-API-Key")
		if key == "" || h.cfg == nil || key != string(h.cfg.AdminAPIKey) {
			httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "unauthorized")
			return
		}
		user := AuthenticatedUser{
			UserID:     apiKeyPrincipalID(key),
			Role:       RoleAdmin,
			AuthSource: "api_key",
		}
		ctx := context.WithValue(r.Context(), UserContextKey, user)
		next(w, r.WithContext(ctx))
	}
}

// createCustomer handles POST /admin/customers for onboarding billing accounts.
func (h *Handler) createCustomer(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	var req struct {
		ID           uuid.UUID `json:"id"`
		Name         string    `json:"name"`
		BalanceMicro *int64    `json:"balance_micro"`
		Balance      *float64  `json:"balance"`
		Currency     string    `json:"currency"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	if req.ID == uuid.Nil {
		req.ID, err = uuid.NewV7()
		if err != nil {
			httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to generate secure customer id")
			return
		}
	}
	legacy := 0.0
	hasLegacy := req.Balance != nil
	if hasLegacy {
		legacy = *req.Balance
	}
	balanceMicro, err := parseMoneyMicro(req.BalanceMicro, legacy, hasLegacy, "balance")
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	if err := h.svc.CreateCustomer(r.Context(), req.ID, req.Name, balanceMicro, req.Currency); err != nil {
		writeServiceError(w, err, slog.String("customer_id", req.ID.String()))
		return
	}
	httpresponse.JSON(w, http.StatusCreated, map[string]any{"id": req.ID})
}

// topUpBalance handles POST /admin/customers/{id}/topup for idempotent balance credits.
func (h *Handler) topUpBalance(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	customerID, err := uuid.Parse(idStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer id")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	var req struct {
		AmountMicro *int64   `json:"amount_micro"`
		Amount      *float64 `json:"amount"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	legacy := 0.0
	hasLegacy := req.Amount != nil
	if hasLegacy {
		legacy = *req.Amount
	}
	amountMicro, err := parseMoneyMicro(req.AmountMicro, legacy, hasLegacy, "amount")
	if err != nil || amountMicro <= 0 {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "amount is required")
		return
	}
	hash := h.svc.GenerateIdempotencyHash(customerID, req)
	if err := h.svc.TopUpBalance(r.Context(), customerID, amountMicro, hash); err != nil {
		writeServiceError(w, err, slog.String("customer_id", customerID.String()))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// createCampaign handles POST /admin/campaigns for launching new delivery with budget reservation.
func (h *Handler) createCampaign(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	var req struct {
		CustomerID       uuid.UUID  `json:"customer_id"`
		BrandID          *uuid.UUID `json:"brand_id,omitempty"`
		Name             string     `json:"name"`
		BudgetLimitMicro *int64     `json:"budget_limit_micro"`
		BudgetLimit      *float64   `json:"budget_limit"`
		PacingMode       string     `json:"pacing_mode"`
		DailyBudgetMicro *int64     `json:"daily_budget_micro"`
		DailyBudget      *float64   `json:"daily_budget"`
		Timezone         string     `json:"timezone"`
		FreqLimit        int32      `json:"freq_limit"`
		FreqWindow       int32      `json:"freq_window"`
		TargetCountries  []string   `json:"target_countries"`
		StartAt          *time.Time `json:"start_at,omitempty"`
		EndAt            *time.Time `json:"end_at,omitempty"`
		DaypartHours     []int16    `json:"daypart_hours,omitempty"`
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "failed to read request body")
		return
	}
	if err := json.Unmarshal(body, &req); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	if req.CustomerID == uuid.Nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "customer_id is required")
		return
	}

	u, ok := GetUser(r.Context())
	if ok && u.IsUser() && req.CustomerID != u.CustomerID {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden: cannot create campaign for another customer")
		return
	}

	pacing := db.PacingModeTypeASAP
	if req.PacingMode == "EVEN" {
		pacing = db.PacingModeTypeEVEN
	}
	if req.Timezone == "" {
		req.Timezone = "UTC"
	}
	if req.FreqWindow == 0 {
		req.FreqWindow = 86400
	}

	budgetLegacy := 0.0
	hasBudgetLegacy := req.BudgetLimit != nil
	if hasBudgetLegacy {
		budgetLegacy = *req.BudgetLimit
	}
	budgetLimitMicro, err := parseBudgetMicro(req.BudgetLimitMicro, budgetLegacy, hasBudgetLegacy)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	dailyLegacy := 0.0
	hasDailyLegacy := req.DailyBudget != nil
	if hasDailyLegacy {
		dailyLegacy = *req.DailyBudget
	}
	dailyBudgetMicro, err := parseMoneyMicro(req.DailyBudgetMicro, dailyLegacy, hasDailyLegacy, "daily_budget")
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}

	hash := h.svc.GenerateIdempotencyHash(req.CustomerID, body)
	id, err := h.svc.CreateCampaign(r.Context(), CampaignCreateSpec{
		CustomerID:      req.CustomerID,
		BrandID:         req.BrandID,
		Name:            req.Name,
		BudgetLimit:     budgetLimitMicro,
		PacingMode:      pacing,
		DailyBudget:     dailyBudgetMicro,
		Timezone:        req.Timezone,
		FreqLimit:       req.FreqLimit,
		FreqWindow:      req.FreqWindow,
		TargetCountries: req.TargetCountries,
		StartAt:         req.StartAt,
		EndAt:           req.EndAt,
		DaypartHours:    req.DaypartHours,
		IdempotencyKey:  hash,
	})
	if err != nil {
		writeServiceError(w, err, slog.String("customer_id", req.CustomerID.String()))
		return
	}
	httpresponse.JSON(w, http.StatusCreated, map[string]any{"id": id})
}

// cancelCampaign handles DELETE /admin/campaigns/{id} for graceful campaign shutdown.
func (h *Handler) cancelCampaign(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	campaignID, err := uuid.Parse(idStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid campaign id")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	var req struct {
		Reason string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		slog.Warn("failed to decode cancel campaign request", "error", err)
	}

	u, ok := GetUser(r.Context())
	if ok && u.IsUser() {
		camp, errCamp := h.svc.GetCampaign(r.Context(), campaignID)
		if errCamp != nil || uuid.UUID(camp.CustomerID.Bytes) != u.CustomerID {
			httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden: campaign belongs to another customer")
			return
		}
	}

	if err := h.svc.CancelCampaign(r.Context(), campaignID, req.Reason); err != nil {
		writeServiceError(w, err, slog.String("campaign_id", campaignID.String()))
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// updateCampaignPacing handles POST /admin/campaigns/{id}/pacing for manual ASAP or EVEN selection.
func (h *Handler) updateCampaignPacing(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	campaignID, err := uuid.Parse(idStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid campaign id")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	var req struct {
		PacingMode string `json:"pacing_mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		slog.Warn("failed to decode update campaign pacing request", "error", err)
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}

	if req.PacingMode != "ASAP" && req.PacingMode != "EVEN" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "pacing_mode must be ASAP or EVEN")
		return
	}

	u, ok := GetUser(r.Context())
	if ok && u.IsUser() {
		camp, errCamp := h.svc.GetCampaign(r.Context(), campaignID)
		if errCamp != nil || uuid.UUID(camp.CustomerID.Bytes) != u.CustomerID {
			httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden: campaign belongs to another customer")
			return
		}
	}

	updatedCamp, err := h.svc.UpdateCampaignPacing(r.Context(), campaignID, req.PacingMode)
	if err != nil {
		writeServiceError(w, err, slog.String("campaign_id", campaignID.String()))
		return
	}

	httpresponse.JSON(w, http.StatusOK, updatedCamp)
}

// updateSettings handles POST /admin/settings for system configuration changes.
func (h *Handler) updateSettings(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	var settings map[string]string
	if err := json.NewDecoder(r.Body).Decode(&settings); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	if err := h.svc.UpdateSettings(r.Context(), settings); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// toggleEmergencyBreaker handles POST /admin/system/breaker for the global ad delivery kill switch.
func (h *Handler) toggleEmergencyBreaker(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	var req struct {
		Active bool   `json:"active"`
		Reason string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	if err := h.svc.ToggleEmergencyBreaker(r.Context(), req.Active, req.Reason); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// blockIP handles POST /admin/blacklist for operator-initiated IP blocks.
func (h *Handler) blockIP(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	var req struct {
		IP     string `json:"ip"`
		Source string `json:"source"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.IP == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	if err := h.svc.BlockIP(r.Context(), req.IP, req.Source); err != nil {
		writeServiceError(w, err, slog.String("ip", req.IP))
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// unblockIP handles DELETE /admin/blacklist for removing blocked IPs.
func (h *Handler) unblockIP(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	var req struct {
		IP     string `json:"ip"`
		Source string `json:"source"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.IP == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	if err := h.svc.UnblockIP(r.Context(), req.IP, req.Source); err != nil {
		writeServiceError(w, err, slog.String("ip", req.IP))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// listAudit handles GET /admin/audit for compliance review of admin actions.
func (h *Handler) listAudit(w http.ResponseWriter, r *http.Request) {
	limit, offset := parsePagination(r)
	logs, err := h.svc.ListAuditLogs(r.Context(), limit, offset)
	if err != nil {
		slog.Error("failed to list audit logs", "error", err)
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "internal error")
		return
	}

	httpresponse.JSON(w, http.StatusOK, logs)
}

// parsePagination reads limit and offset query params with safe defaults and caps.
func parsePagination(r *http.Request) (int32, int32) {
	limit := int32(20)
	if l, err := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 32); err == nil && l > 0 {
		limit = int32(l)
		if limit > 100 {
			limit = 100
		}
	}
	offset := int32(0)
	if o, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 32); err == nil && o > 0 {
		offset = int32(o)
	}
	return limit, offset
}

// listCustomers handles GET /admin/customers for paginated account listing.
func (h *Handler) listCustomers(w http.ResponseWriter, r *http.Request) {
	limit, offset := parsePagination(r)
	customers, total, err := h.svc.ListCustomers(r.Context(), limit, offset)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	w.Header().Set("X-Total-Count", strconv.FormatInt(total, 10))
	httpresponse.JSON(w, http.StatusOK, customers)
}

// getCustomer handles GET /admin/customers/{id} for account detail with spend stats.
func (h *Handler) getCustomer(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	customerID, err := uuid.Parse(idStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer id")
		return
	}

	u, ok := GetUser(r.Context())
	if ok && u.IsUser() && u.CustomerID != customerID {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden: cannot access another customer")
		return
	}

	customer, err := h.svc.GetCustomerDTO(r.Context(), customerID)
	if err != nil {
		httpresponse.Error(w, http.StatusNotFound, "NOT_FOUND", "customer not found")
		return
	}

	httpresponse.JSON(w, http.StatusOK, customer)
}

// getCustomerLedger handles GET /admin/customers/{id}/ledger for billing history.
func (h *Handler) getCustomerLedger(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	customerID, err := uuid.Parse(idStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer id")
		return
	}

	u, ok := GetUser(r.Context())
	if ok && u.IsUser() && u.CustomerID != customerID {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden: cannot access another customer")
		return
	}

	limit, offset := parsePagination(r)
	ledger, total, err := h.svc.ListCustomerLedger(r.Context(), customerID, limit, offset)
	if err != nil {
		writeServiceError(w, err, slog.String("customer_id", customerID.String()))
		return
	}

	w.Header().Set("X-Total-Count", strconv.FormatInt(total, 10))
	httpresponse.JSON(w, http.StatusOK, ledger)
}

// listCampaigns handles GET /admin/campaigns with optional customer and status filters.
func (h *Handler) listCampaigns(w http.ResponseWriter, r *http.Request) {
	limit, offset := parsePagination(r)
	status := r.URL.Query().Get("status")

	var custID uuid.UUID
	if cStr := r.URL.Query().Get("customer_id"); cStr != "" {
		if id, err := uuid.Parse(cStr); err == nil {
			custID = id
		}
	}

	u, ok := GetUser(r.Context())
	if ok && u.IsUser() {
		custID = u.CustomerID
	}

	campaigns, total, err := h.svc.ListCampaigns(r.Context(), custID, status, limit, offset)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	w.Header().Set("X-Total-Count", strconv.FormatInt(total, 10))
	httpresponse.JSON(w, http.StatusOK, campaigns)
}

// getCampaign handles GET /admin/campaigns/{id} for campaign detail views.
func (h *Handler) getCampaign(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	campaignID, err := uuid.Parse(idStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid campaign id")
		return
	}

	campaign, err := h.svc.GetCampaignDTO(r.Context(), campaignID)
	if err != nil {
		httpresponse.Error(w, http.StatusNotFound, "NOT_FOUND", "campaign not found")
		return
	}

	u, ok := GetUser(r.Context())
	if ok && u.IsUser() && campaign.CustomerID != u.CustomerID.String() {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden: cannot access another customer's campaign")
		return
	}

	httpresponse.JSON(w, http.StatusOK, campaign)
}

// getCampaignHistory handles GET /admin/campaigns/{id}/history for status transition audit.
func (h *Handler) getCampaignHistory(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	campaignID, err := uuid.Parse(idStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid campaign id")
		return
	}

	u, ok := GetUser(r.Context())
	if ok && u.IsUser() {
		camp, errCamp := h.svc.GetCampaign(r.Context(), campaignID)
		if errCamp != nil || uuid.UUID(camp.CustomerID.Bytes) != u.CustomerID {
			httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden: cannot access another customer's campaign")
			return
		}
	}

	limit, offset := parsePagination(r)
	history, total, err := h.svc.ListStatusHistory(r.Context(), campaignID, limit, offset)
	if err != nil {
		writeServiceError(w, err, slog.String("campaign_id", campaignID.String()))
		return
	}

	w.Header().Set("X-Total-Count", strconv.FormatInt(total, 10))
	httpresponse.JSON(w, http.StatusOK, history)
}

// listBlacklist handles GET /admin/blacklist for blocked IP inventory.
func (h *Handler) listBlacklist(w http.ResponseWriter, r *http.Request) {
	limit, offset := parsePagination(r)
	items, total, err := h.svc.ListBlacklist(r.Context(), limit, offset)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	w.Header().Set("X-Total-Count", strconv.FormatInt(total, 10))
	httpresponse.JSON(w, http.StatusOK, items)
}

// getSettings handles GET /admin/settings for reading system configuration.
func (h *Handler) getSettings(w http.ResponseWriter, r *http.Request) {
	settings, err := h.svc.GetSettings(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}

	httpresponse.JSON(w, http.StatusOK, settings)
}

// createBrand handles POST /admin/brands for registering advertiser brands.
func (h *Handler) createBrand(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	var req struct {
		CustomerID uuid.UUID `json:"customer_id"`
		Name       string    `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	if req.CustomerID == uuid.Nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "customer_id is required")
		return
	}
	if req.Name == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "name is required")
		return
	}

	u, ok := GetUser(r.Context())
	if ok && u.IsUser() && req.CustomerID != u.CustomerID {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden: cannot create brand for another customer")
		return
	}

	id, err := h.svc.CreateBrand(r.Context(), req.CustomerID, req.Name)
	if err != nil {
		writeServiceError(w, err, slog.String("customer_id", req.CustomerID.String()))
		return
	}
	httpresponse.JSON(w, http.StatusCreated, map[string]any{"id": id})
}

// listBrands handles GET /admin/brands for a customer's brand inventory.
func (h *Handler) listBrands(w http.ResponseWriter, r *http.Request) {
	var custID uuid.UUID
	if cStr := r.URL.Query().Get("customer_id"); cStr != "" {
		if id, err := uuid.Parse(cStr); err == nil {
			custID = id
		}
	}

	u, ok := GetUser(r.Context())
	if ok && u.IsUser() {
		custID = u.CustomerID
	}

	if custID == uuid.Nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "customer_id is required")
		return
	}

	brands, err := h.svc.ListBrandsByCustomer(r.Context(), custID)
	if err != nil {
		writeServiceError(w, err, slog.String("customer_id", custID.String()))
		return
	}

	httpresponse.JSON(w, http.StatusOK, brands)
}

// configureBrandFcap handles POST /admin/brands/{id}/fcap for brand-level frequency caps.
func (h *Handler) configureBrandFcap(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	idStr := r.PathValue("id")
	brandID, err := uuid.Parse(idStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid brand id")
		return
	}

	var req struct {
		FreqLimit  int32 `json:"freq_limit"`
		FreqWindow int32 `json:"freq_window"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}

	if req.FreqLimit < 0 || req.FreqWindow < 0 {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "limits must be non-negative")
		return
	}

	u, ok := GetUser(r.Context())
	if ok && u.IsUser() {
		brand, errBrand := h.svc.GetBrandDTO(r.Context(), brandID)
		if errBrand != nil {
			httpresponse.Error(w, http.StatusNotFound, "NOT_FOUND", "brand not found")
			return
		}
		if brand.CustomerID != u.CustomerID.String() {
			httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden: cannot modify brand belonging to another customer")
			return
		}
	}

	err = h.svc.ConfigureBrandFcap(r.Context(), brandID, req.FreqLimit, req.FreqWindow)
	if err != nil {
		writeServiceError(w, err, slog.String("brand_id", brandID.String()))
		return
	}

	w.WriteHeader(http.StatusOK)
}
