package management

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"espx/internal/ads/db"
	"espx/internal/config"
	"espx/pkg/httpresponse"
	"github.com/google/uuid"
	"golang.org/x/time/rate"
)

type Handler struct {
	svc            *Service
	cfg            *config.Config
	limiter        *rate.Limiter
	authMiddleware *AuthMiddleware
}

func NewHandler(svc *Service, cfg *config.Config, authMiddleware *AuthMiddleware) *Handler {
	return &Handler{
		svc:            svc,
		cfg:            cfg,
		limiter:        rate.NewLimiter(rate.Limit(10), 50),
		authMiddleware: authMiddleware,
	}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /admin/customers", h.limit(h.auth(h.createCustomer, "SA", "M")))
	mux.HandleFunc("POST /admin/customers/{id}/topup", h.limit(h.auth(h.topUpBalance, "SA", "M")))
	mux.HandleFunc("POST /admin/campaigns", h.limit(h.auth(h.createCampaign, "SA", "M", "C")))
	mux.HandleFunc("POST /admin/brands", h.limit(h.auth(h.createBrand, "SA", "M", "C")))
	mux.HandleFunc("GET /admin/brands", h.limit(h.auth(h.listBrands, "SA", "M", "C")))
	mux.HandleFunc("POST /admin/brands/{id}/fcap", h.limit(h.auth(h.configureBrandFcap, "SA", "M", "C")))
	mux.HandleFunc("DELETE /admin/campaigns/{id}", h.limit(h.auth(h.cancelCampaign, "SA", "M", "C")))
	mux.HandleFunc("POST /admin/campaigns/{id}/pacing", h.limit(h.auth(h.updateCampaignPacing, "SA", "M", "C")))

	mux.HandleFunc("POST /admin/settings", h.limit(h.auth(h.updateSettings, "SA")))
	mux.HandleFunc("POST /admin/blacklist", h.limit(h.auth(h.blockIP, "SA")))
	mux.HandleFunc("DELETE /admin/blacklist", h.limit(h.auth(h.unblockIP, "SA")))
	mux.HandleFunc("GET /admin/audit", h.limit(h.auth(h.listAudit, "SA", "M")))
	mux.HandleFunc("POST /admin/system/breaker", h.limit(h.auth(h.toggleEmergencyBreaker, "SA")))

	mux.HandleFunc("GET /admin/customers", h.limit(h.auth(h.listCustomers, "SA", "M")))
	mux.HandleFunc("GET /admin/customers/{id}", h.limit(h.auth(h.getCustomer, "SA", "M", "C")))
	mux.HandleFunc("GET /admin/customers/{id}/ledger", h.limit(h.auth(h.getCustomerLedger, "SA", "M", "C")))

	mux.HandleFunc("GET /admin/campaigns", h.limit(h.auth(h.listCampaigns, "SA", "M", "C")))
	mux.HandleFunc("GET /admin/campaigns/{id}", h.limit(h.auth(h.getCampaign, "SA", "M", "C")))
	mux.HandleFunc("GET /admin/campaigns/{id}/history", h.limit(h.auth(h.getCampaignHistory, "SA", "M", "C")))

	mux.HandleFunc("GET /admin/blacklist", h.limit(h.auth(h.listBlacklist, "SA")))
	mux.HandleFunc("GET /admin/settings", h.limit(h.auth(h.getSettings, "SA")))
}

func (h *Handler) limit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !h.limiter.Allow() {
			httpresponse.Error(w, http.StatusTooManyRequests, "TOO_MANY_REQUESTS", "too many requests")
			return
		}
		next(w, r)
	}
}

func (h *Handler) auth(next http.HandlerFunc, allowedRoles ...string) http.HandlerFunc {
	if h.authMiddleware != nil {
		return h.authMiddleware.RequireAuth(allowedRoles...)(next)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-Admin-API-Key")
		if key == "" || key != string(h.cfg.AdminAPIKey) {
			httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "unauthorized")
			return
		}
		next(w, r)
	}
}

func (h *Handler) createCustomer(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	var req struct {
		ID       uuid.UUID `json:"id"`
		Name     string    `json:"name"`
		Balance  float64   `json:"balance"`
		Currency string    `json:"currency"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	if req.ID == uuid.Nil {
		var err error
		req.ID, err = uuid.NewV7()
		if err != nil {
			httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to generate secure customer id")
			return
		}
	}
	balanceMicro := int64(req.Balance * 1_000_000)
	if err := h.svc.CreateCustomer(r.Context(), req.ID, req.Name, balanceMicro, req.Currency); err != nil {
		slog.Error("failed to create customer", "error", err)
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	httpresponse.JSON(w, http.StatusCreated, map[string]any{"id": req.ID})
}

func (h *Handler) topUpBalance(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	customerID, err := uuid.Parse(idStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer id")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	var req struct {
		Amount float64 `json:"amount"`
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
	hash := h.svc.GenerateIdempotencyHash(customerID, req)
	amountMicro := int64(req.Amount * 1_000_000)
	if err := h.svc.TopUpBalance(r.Context(), customerID, amountMicro, hash); err != nil {
		slog.Error("failed to top up balance", "error", err, "customer_id", customerID)
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) createCampaign(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	var req struct {
		CustomerID      uuid.UUID  `json:"customer_id"`
		BrandID         *uuid.UUID `json:"brand_id,omitempty"`
		Name            string     `json:"name"`
		BudgetLimit     float64    `json:"budget_limit"`
		PacingMode      string     `json:"pacing_mode"`
		DailyBudget     float64    `json:"daily_budget"`
		Timezone        string     `json:"timezone"`
		FreqLimit       int32      `json:"freq_limit"`
		FreqWindow      int32      `json:"freq_window"`
		TargetCountries []string   `json:"target_countries"`
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
	if ok && u.Role == "C" && req.CustomerID != u.CustomerID {
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

	hash := h.svc.GenerateIdempotencyHash(req.CustomerID, req)
	budgetLimitMicro := int64(req.BudgetLimit * 1_000_000)
	dailyBudgetMicro := int64(req.DailyBudget * 1_000_000)

	id, err := h.svc.CreateCampaign(r.Context(), req.CustomerID, req.BrandID, req.Name, budgetLimitMicro, pacing, dailyBudgetMicro, req.Timezone, req.FreqLimit, req.FreqWindow, req.TargetCountries, hash)
	if err != nil {
		slog.Error("failed to create campaign", "error", err, "customer_id", req.CustomerID)
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	httpresponse.JSON(w, http.StatusCreated, map[string]any{"id": id})
}

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
	if ok && u.Role == "C" {
		camp, errCamp := h.svc.GetCampaign(r.Context(), campaignID)
		if errCamp != nil || uuid.UUID(camp.CustomerID.Bytes) != u.CustomerID {
			httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden: campaign belongs to another customer")
			return
		}
	}

	if err := h.svc.CancelCampaign(r.Context(), campaignID, req.Reason); err != nil {
		slog.Error("failed to cancel campaign", "error", err, "campaign_id", campaignID)
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

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
	if ok && u.Role == "C" {
		camp, errCamp := h.svc.GetCampaign(r.Context(), campaignID)
		if errCamp != nil || uuid.UUID(camp.CustomerID.Bytes) != u.CustomerID {
			httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden: campaign belongs to another customer")
			return
		}
	}

	updatedCamp, err := h.svc.UpdateCampaignPacing(r.Context(), campaignID, req.PacingMode)
	if err != nil {
		slog.Error("failed to update campaign pacing", "error", err, "campaign_id", campaignID)
		if strings.Contains(err.Error(), "campaign not found") {
			httpresponse.Error(w, http.StatusNotFound, "NOT_FOUND", "campaign not found")
		} else {
			httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		}
		return
	}

	httpresponse.JSON(w, http.StatusOK, updatedCamp)
}

func (h *Handler) updateSettings(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	var settings map[string]string
	if err := json.NewDecoder(r.Body).Decode(&settings); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	if err := h.svc.UpdateSettings(r.Context(), settings); err != nil {
		slog.Error("failed to update settings", "error", err)
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

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
		slog.Error("failed to toggle emergency breaker", "error", err)
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

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
		slog.Error("failed to block ip", "error", err, "ip", req.IP)
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	w.WriteHeader(http.StatusCreated)
}

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
		slog.Error("failed to unblock ip", "error", err, "ip", req.IP)
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

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

func (h *Handler) listCustomers(w http.ResponseWriter, r *http.Request) {
	limit, offset := parsePagination(r)
	customers, total, err := h.svc.ListCustomers(r.Context(), limit, offset)
	if err != nil {
		slog.Error("failed to list customers", "error", err)
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	w.Header().Set("X-Total-Count", strconv.FormatInt(total, 10))
	httpresponse.JSON(w, http.StatusOK, customers)
}

func (h *Handler) getCustomer(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	customerID, err := uuid.Parse(idStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer id")
		return
	}

	u, ok := GetUser(r.Context())
	if ok && u.Role == "C" && u.CustomerID != customerID {
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

func (h *Handler) getCustomerLedger(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	customerID, err := uuid.Parse(idStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer id")
		return
	}

	u, ok := GetUser(r.Context())
	if ok && u.Role == "C" && u.CustomerID != customerID {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden: cannot access another customer")
		return
	}

	limit, offset := parsePagination(r)
	ledger, total, err := h.svc.ListCustomerLedger(r.Context(), customerID, limit, offset)
	if err != nil {
		slog.Error("failed to list customer ledger", "error", err)
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	w.Header().Set("X-Total-Count", strconv.FormatInt(total, 10))
	httpresponse.JSON(w, http.StatusOK, ledger)
}

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
	if ok && u.Role == "C" {
		custID = u.CustomerID
	}

	campaigns, total, err := h.svc.ListCampaigns(r.Context(), custID, status, limit, offset)
	if err != nil {
		slog.Error("failed to list campaigns", "error", err)
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	w.Header().Set("X-Total-Count", strconv.FormatInt(total, 10))
	httpresponse.JSON(w, http.StatusOK, campaigns)
}

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
	if ok && u.Role == "C" && campaign.CustomerID != u.CustomerID.String() {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden: cannot access another customer's campaign")
		return
	}

	httpresponse.JSON(w, http.StatusOK, campaign)
}

func (h *Handler) getCampaignHistory(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	campaignID, err := uuid.Parse(idStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid campaign id")
		return
	}

	u, ok := GetUser(r.Context())
	if ok && u.Role == "C" {
		camp, errCamp := h.svc.GetCampaign(r.Context(), campaignID)
		if errCamp != nil || uuid.UUID(camp.CustomerID.Bytes) != u.CustomerID {
			httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden: cannot access another customer's campaign")
			return
		}
	}

	limit, offset := parsePagination(r)
	history, total, err := h.svc.ListStatusHistory(r.Context(), campaignID, limit, offset)
	if err != nil {
		slog.Error("failed to list campaign history", "error", err)
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	w.Header().Set("X-Total-Count", strconv.FormatInt(total, 10))
	httpresponse.JSON(w, http.StatusOK, history)
}

func (h *Handler) listBlacklist(w http.ResponseWriter, r *http.Request) {
	limit, offset := parsePagination(r)
	items, total, err := h.svc.ListBlacklist(r.Context(), limit, offset)
	if err != nil {
		slog.Error("failed to list blacklist", "error", err)
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	w.Header().Set("X-Total-Count", strconv.FormatInt(total, 10))
	httpresponse.JSON(w, http.StatusOK, items)
}

func (h *Handler) getSettings(w http.ResponseWriter, r *http.Request) {
	settings, err := h.svc.GetSettings(r.Context())
	if err != nil {
		slog.Error("failed to get settings", "error", err)
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	httpresponse.JSON(w, http.StatusOK, settings)
}

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
	if ok && u.Role == "C" && req.CustomerID != u.CustomerID {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden: cannot create brand for another customer")
		return
	}

	id, err := h.svc.CreateBrand(r.Context(), req.CustomerID, req.Name)
	if err != nil {
		slog.Error("failed to create brand", "error", err, "customer_id", req.CustomerID)
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	httpresponse.JSON(w, http.StatusCreated, map[string]any{"id": id})
}

func (h *Handler) listBrands(w http.ResponseWriter, r *http.Request) {
	var custID uuid.UUID
	if cStr := r.URL.Query().Get("customer_id"); cStr != "" {
		if id, err := uuid.Parse(cStr); err == nil {
			custID = id
		}
	}

	u, ok := GetUser(r.Context())
	if ok && u.Role == "C" {
		custID = u.CustomerID
	}

	if custID == uuid.Nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "customer_id is required")
		return
	}

	brands, err := h.svc.ListBrandsByCustomer(r.Context(), custID)
	if err != nil {
		slog.Error("failed to list brands", "error", err, "customer_id", custID)
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	httpresponse.JSON(w, http.StatusOK, brands)
}

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
	if ok && u.Role == "C" {
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
		slog.Error("failed to configure brand fcap", "error", err, "brand_id", brandID)
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
}
