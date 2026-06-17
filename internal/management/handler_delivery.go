package management

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"espx/internal/ads/db"
	"espx/pkg/httpresponse"
	"github.com/google/uuid"
)

// registerDeliveryRoutes mounts template, schedule, pause, resume, and creative management endpoints.
func (h *Handler) registerDeliveryRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /admin/campaign-templates", h.limit(h.perm(h.createCampaignTemplate, PermCampaignsWrite)))
	mux.HandleFunc("GET /admin/campaign-templates", h.limit(h.perm(h.listCampaignTemplates, PermCampaignsRead)))
	mux.HandleFunc("POST /admin/campaign-templates/{id}/instantiate", h.limit(h.perm(h.createCampaignFromTemplate, PermCampaignsWrite)))
	mux.HandleFunc("POST /admin/campaigns/{id}/save-as-template", h.limit(h.perm(h.saveCampaignAsTemplate, PermCampaignsWrite)))

	mux.HandleFunc("POST /admin/campaigns/{id}/pause", h.limit(h.perm(h.pauseCampaign, PermCampaignsWrite)))
	mux.HandleFunc("POST /admin/campaigns/{id}/resume", h.limit(h.perm(h.resumeCampaign, PermCampaignsWrite)))
	mux.HandleFunc("POST /admin/campaigns/{id}/schedule", h.limit(h.perm(h.updateCampaignSchedule, PermCampaignsWrite)))

	mux.HandleFunc("POST /admin/brands/{id}/creatives", h.limit(h.perm(h.createBrandCreative, PermBrandsWrite)))
	mux.HandleFunc("GET /admin/brands/{id}/creatives", h.limit(h.perm(h.listBrandCreatives, PermBrandsRead)))
	mux.HandleFunc("PUT /admin/brands/{brand_id}/creatives/{id}", h.limit(h.perm(h.updateBrandCreative, PermBrandsWrite)))
	mux.HandleFunc("DELETE /admin/brands/{brand_id}/creatives/{id}", h.limit(h.perm(h.deleteBrandCreative, PermBrandsWrite)))
}

// createCampaignTemplate handles POST /admin/campaign-templates for saving reusable campaign presets.
func (h *Handler) createCampaignTemplate(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	var req struct {
		CustomerID       uuid.UUID  `json:"customer_id"`
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
		BrandID          *uuid.UUID `json:"brand_id,omitempty"`
		DaypartHours     []int16    `json:"daypart_hours"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.CustomerID == uuid.Nil || req.Name == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	if u, ok := GetUser(r.Context()); ok && u.IsUser() && req.CustomerID != u.CustomerID {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden")
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
	hasBudget := req.BudgetLimit != nil
	if hasBudget {
		budgetLegacy = *req.BudgetLimit
	}
	budgetMicro, err := parseBudgetMicro(req.BudgetLimitMicro, budgetLegacy, hasBudget)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	dailyLegacy := 0.0
	hasDaily := req.DailyBudget != nil
	if hasDaily {
		dailyLegacy = *req.DailyBudget
	}
	dailyMicro, err := parseMoneyMicro(req.DailyBudgetMicro, dailyLegacy, hasDaily, "daily_budget")
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	id, err := h.svc.CreateCampaignTemplate(r.Context(), req.CustomerID, req.Name, budgetMicro, pacing, dailyMicro, req.Timezone, req.FreqLimit, req.FreqWindow, req.TargetCountries, req.BrandID, req.DaypartHours)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusCreated, map[string]any{"id": id})
}

// listCampaignTemplates handles GET /admin/campaign-templates for a customer's template library.
func (h *Handler) listCampaignTemplates(w http.ResponseWriter, r *http.Request) {
	var custID uuid.UUID
	if cStr := r.URL.Query().Get("customer_id"); cStr != "" {
		custID, _ = uuid.Parse(cStr)
	}
	if u, ok := GetUser(r.Context()); ok && u.IsUser() {
		custID = u.CustomerID
	}
	if custID == uuid.Nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "customer_id is required")
		return
	}
	limit, offset := parsePagination(r)
	items, total, err := h.svc.ListCampaignTemplates(r.Context(), custID, limit, offset)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	w.Header().Set("X-Total-Count", strconv.FormatInt(total, 10))
	httpresponse.JSON(w, http.StatusOK, items)
}

// createCampaignFromTemplate handles POST /admin/campaign-templates/{id}/instantiate for launching from a preset.
func (h *Handler) createCampaignFromTemplate(w http.ResponseWriter, r *http.Request) {
	templateID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid template id")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	body, _ := io.ReadAll(r.Body)
	var req struct {
		CustomerID       uuid.UUID `json:"customer_id"`
		Name             string    `json:"name"`
		BudgetLimitMicro *int64    `json:"budget_limit_micro"`
		BudgetLimit      *float64  `json:"budget_limit"`
	}
	_ = json.Unmarshal(body, &req)
	if req.CustomerID == uuid.Nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "customer_id is required")
		return
	}
	if u, ok := GetUser(r.Context()); ok && u.IsUser() && req.CustomerID != u.CustomerID {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden")
		return
	}
	budgetMicro, err := optionalBudgetMicro(req.BudgetLimitMicro, req.BudgetLimit)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	hash := h.svc.GenerateIdempotencyHash(req.CustomerID, body)
	id, err := h.svc.CreateCampaignFromTemplate(r.Context(), templateID, req.CustomerID, req.Name, budgetMicro, hash)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusCreated, map[string]any{"id": id})
}

// saveCampaignAsTemplate handles POST /admin/campaigns/{id}/save-as-template for snapshotting live config.
func (h *Handler) saveCampaignAsTemplate(w http.ResponseWriter, r *http.Request) {
	campaignID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid campaign id")
		return
	}
	if u, ok := GetUser(r.Context()); ok && u.IsUser() {
		camp, errCamp := h.svc.GetCampaign(r.Context(), campaignID)
		if errCamp != nil || uuid.UUID(camp.CustomerID.Bytes) != u.CustomerID {
			httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden")
			return
		}
	}
	var req struct {
		Name string `json:"name"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	id, err := h.svc.SaveCampaignAsTemplate(r.Context(), campaignID, req.Name)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusCreated, map[string]any{"id": id})
}

// pauseCampaign handles POST /admin/campaigns/{id}/pause for operator-initiated delivery stop.
func (h *Handler) pauseCampaign(w http.ResponseWriter, r *http.Request) {
	campaignID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid campaign id")
		return
	}
	if err := h.ensureCampaignAccess(r, campaignID); err != nil {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden")
		return
	}
	var req struct {
		Reason string `json:"reason"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if err := h.svc.PauseCampaign(r.Context(), campaignID, req.Reason); err != nil {
		writeServiceError(w, err, slog.String("campaign_id", campaignID.String()))
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// resumeCampaign handles POST /admin/campaigns/{id}/resume for operator-initiated delivery restart.
func (h *Handler) resumeCampaign(w http.ResponseWriter, r *http.Request) {
	campaignID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid campaign id")
		return
	}
	if err := h.ensureCampaignAccess(r, campaignID); err != nil {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden")
		return
	}
	var req struct {
		Reason string `json:"reason"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if err := h.svc.ResumeCampaign(r.Context(), campaignID, req.Reason); err != nil {
		writeServiceError(w, err, slog.String("campaign_id", campaignID.String()))
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// updateCampaignSchedule handles POST /admin/campaigns/{id}/schedule for window and daypart changes.
func (h *Handler) updateCampaignSchedule(w http.ResponseWriter, r *http.Request) {
	campaignID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid campaign id")
		return
	}
	if err := h.ensureCampaignAccess(r, campaignID); err != nil {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden")
		return
	}
	var req struct {
		StartAt      *time.Time `json:"start_at"`
		EndAt        *time.Time `json:"end_at"`
		DaypartHours []int16    `json:"daypart_hours"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	if err := h.svc.UpdateCampaignSchedule(r.Context(), campaignID, req.StartAt, req.EndAt, req.DaypartHours); err != nil {
		writeServiceError(w, err, slog.String("campaign_id", campaignID.String()))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// createBrandCreative handles POST /admin/brands/{id}/creatives for adding a weighted landing URL.
func (h *Handler) createBrandCreative(w http.ResponseWriter, r *http.Request) {
	brandID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid brand id")
		return
	}
	if err := h.ensureBrandAccess(r, brandID); err != nil {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden")
		return
	}
	var req struct {
		Name       string `json:"name"`
		LandingURL string `json:"landing_url"`
		Weight     int32  `json:"weight"`
		Status     string `json:"status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" || req.LandingURL == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	if req.Weight <= 0 {
		req.Weight = 100
	}
	id, err := h.svc.UpsertBrandCreative(r.Context(), brandID, req.Name, req.LandingURL, req.Weight, req.Status)
	if err != nil {
		writeServiceError(w, err, slog.String("brand_id", brandID.String()))
		return
	}
	httpresponse.JSON(w, http.StatusCreated, map[string]any{"id": id})
}

// listBrandCreatives handles GET /admin/brands/{id}/creatives for creative inventory.
func (h *Handler) listBrandCreatives(w http.ResponseWriter, r *http.Request) {
	brandID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid brand id")
		return
	}
	if err := h.ensureBrandAccess(r, brandID); err != nil {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden")
		return
	}
	items, err := h.svc.ListBrandCreatives(r.Context(), brandID)
	if err != nil {
		writeServiceError(w, err, slog.String("brand_id", brandID.String()))
		return
	}
	httpresponse.JSON(w, http.StatusOK, items)
}

// updateBrandCreative handles PUT /admin/brands/{brand_id}/creatives/{id} for editing a creative.
func (h *Handler) updateBrandCreative(w http.ResponseWriter, r *http.Request) {
	brandID, err := uuid.Parse(r.PathValue("brand_id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid brand id")
		return
	}
	creativeID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid creative id")
		return
	}
	if err := h.ensureBrandAccess(r, brandID); err != nil {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden")
		return
	}
	var req struct {
		Name       string `json:"name"`
		LandingURL string `json:"landing_url"`
		Weight     int32  `json:"weight"`
		Status     string `json:"status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	if err := h.svc.UpdateBrandCreative(r.Context(), creativeID, req.Name, req.LandingURL, req.Weight, req.Status); err != nil {
		writeServiceError(w, err, slog.String("creative_id", creativeID.String()))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// deleteBrandCreative handles DELETE /admin/brands/{brand_id}/creatives/{id} for removing a variant.
func (h *Handler) deleteBrandCreative(w http.ResponseWriter, r *http.Request) {
	brandID, err := uuid.Parse(r.PathValue("brand_id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid brand id")
		return
	}
	creativeID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid creative id")
		return
	}
	if err := h.ensureBrandAccess(r, brandID); err != nil {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden")
		return
	}
	if err := h.svc.DeleteBrandCreative(r.Context(), creativeID); err != nil {
		writeServiceError(w, err, slog.String("creative_id", creativeID.String()))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ensureCampaignAccess restricts customer users to campaigns owned by their tenant.
func (h *Handler) ensureCampaignAccess(r *http.Request, campaignID uuid.UUID) error {
	u, ok := GetUser(r.Context())
	if !ok || !u.IsUser() {
		return nil
	}
	camp, err := h.svc.GetCampaign(r.Context(), campaignID)
	if err != nil {
		return err
	}
	if uuid.UUID(camp.CustomerID.Bytes) != u.CustomerID {
		return errForbidden
	}
	return nil
}

// ensureBrandAccess restricts customer users to brands owned by their tenant.
func (h *Handler) ensureBrandAccess(r *http.Request, brandID uuid.UUID) error {
	u, ok := GetUser(r.Context())
	if !ok || !u.IsUser() {
		return nil
	}
	brand, err := h.svc.GetBrandDTO(r.Context(), brandID)
	if err != nil {
		return err
	}
	if brand.CustomerID != u.CustomerID.String() {
		return errForbidden
	}
	return nil
}

var errForbidden = &forbiddenError{}

// forbiddenError signals a tenant isolation violation without leaking resource existence details.
type forbiddenError struct{}

// Error implements error for forbidden tenant access responses.
func (e *forbiddenError) Error() string { return "forbidden" }
