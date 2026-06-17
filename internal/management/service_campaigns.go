package management

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// CampaignDTO exposes campaign state and delivery settings to the admin API.
type CampaignDTO struct {
	ID              string   `json:"id"`
	Name            string   `json:"name"`
	Status          string   `json:"status"`
	BudgetLimit     string   `json:"budget_limit"`
	CurrentSpend    string   `json:"current_spend"`
	CustomerID      string   `json:"customer_id"`
	PacingMode      string   `json:"pacing_mode"`
	DailyBudget     string   `json:"daily_budget"`
	Timezone        string   `json:"timezone"`
	FreqLimit       int32    `json:"freq_limit"`
	FreqWindow      int32    `json:"freq_window"`
	TargetCountries []string `json:"target_countries"`
	StartAt         string   `json:"start_at,omitempty"`
	EndAt           string   `json:"end_at,omitempty"`
	DaypartHours    []int16  `json:"daypart_hours"`
	CreatedAt       string   `json:"created_at"`
	UpdatedAt       string   `json:"updated_at"`
}

// StatusHistoryDTO records a campaign status transition for audit and troubleshooting.
type StatusHistoryDTO struct {
	ID         int64  `json:"id"`
	CampaignID string `json:"campaign_id"`
	OldStatus  string `json:"old_status,omitempty"`
	NewStatus  string `json:"new_status"`
	Reason     string `json:"reason,omitempty"`
	CreatedAt  string `json:"created_at"`
}

// toCampaignDTO maps a database campaign row into the admin API representation.
func toCampaignDTO(c db.Campaign) CampaignDTO {
	countries := c.TargetCountries
	if countries == nil {
		countries = []string{}
	}

	return CampaignDTO{
		ID:              uuid.UUID(c.ID.Bytes).String(),
		Name:            c.Name,
		Status:          string(c.Status),
		BudgetLimit:     formatMicro(c.BudgetLimit),
		CurrentSpend:    formatMicro(c.CurrentSpend),
		CustomerID:      uuid.UUID(c.CustomerID.Bytes).String(),
		PacingMode:      string(c.PacingMode),
		DailyBudget:     formatMicro(c.DailyBudget),
		Timezone:        c.Timezone,
		FreqLimit:       c.FreqLimit.Int32,
		FreqWindow:      c.FreqWindow.Int32,
		TargetCountries: countries,
		StartAt:         formatOptionalTime(c.StartAt),
		EndAt:           formatOptionalTime(c.EndAt),
		DaypartHours:    daypartOrEmpty(c.DaypartHours),
		CreatedAt:       c.CreatedAt.Time.Format(time.RFC3339),
		UpdatedAt:       c.UpdatedAt.Time.Format(time.RFC3339),
	}
}

// formatOptionalTime renders optional schedule timestamps for JSON responses.
func formatOptionalTime(t pgtype.Timestamptz) string {
	if !t.Valid {
		return ""
	}
	return t.Time.Format(time.RFC3339)
}

// daypartOrEmpty normalizes nil daypart slices to empty JSON arrays.
func daypartOrEmpty(h []int16) []int16 {
	if h == nil {
		return []int16{}
	}
	return h
}

// ListCampaigns returns paginated campaigns filtered by customer and status for the admin UI.
func (s *Service) ListCampaigns(ctx context.Context, customerID uuid.UUID, status string, limit, offset int32) ([]CampaignDTO, int64, error) {
	q := db.New(s.pool)

	var cid pgtype.UUID
	if customerID != uuid.Nil {
		cid = ads.ToUUID(customerID)
	}

	var st pgtype.Text
	if status != "" {
		st = pgtype.Text{String: status, Valid: true}
	}

	countParams := db.CountCampaignsParams{
		CustomerID: cid,
		Status:     st,
	}

	total, err := q.CountCampaigns(ctx, countParams)
	if err != nil {
		return nil, 0, err
	}

	if total == 0 {
		return []CampaignDTO{}, 0, nil
	}

	listParams := db.ListCampaignsParams{
		Limit:      limit,
		Offset:     offset,
		CustomerID: cid,
		Status:     st,
	}

	rows, err := q.ListCampaigns(ctx, listParams)
	if err != nil {
		return nil, 0, err
	}

	res := make([]CampaignDTO, len(rows))
	for i, r := range rows {
		res[i] = toCampaignDTO(r)
	}

	return res, total, nil
}

// GetCampaignDTO loads a single campaign for detail views and access checks.
func (s *Service) GetCampaignDTO(ctx context.Context, id uuid.UUID) (CampaignDTO, error) {
	q := db.New(s.pool)
	c, err := q.GetCampaignFull(ctx, ads.ToUUID(id))
	if err != nil {
		return CampaignDTO{}, err
	}
	return toCampaignDTO(c), nil
}

// ListStatusHistory returns paginated status transitions for a campaign audit trail.
func (s *Service) ListStatusHistory(ctx context.Context, campaignID uuid.UUID, limit, offset int32) ([]StatusHistoryDTO, int64, error) {
	q := db.New(s.pool)
	cid := ads.ToUUID(campaignID)

	total, err := q.CountStatusHistory(ctx, cid)
	if err != nil {
		return nil, 0, err
	}
	if total == 0 {
		return []StatusHistoryDTO{}, 0, nil
	}

	rows, err := q.ListStatusHistory(ctx, db.ListStatusHistoryParams{
		CampaignID: cid,
		Limit:      limit,
		Offset:     offset,
	})
	if err != nil {
		return nil, 0, err
	}

	res := make([]StatusHistoryDTO, len(rows))
	for i, r := range rows {
		var oldStatus string
		if r.OldStatus.Valid {
			oldStatus = string(r.OldStatus.CampaignStatusType)
		}
		res[i] = StatusHistoryDTO{
			ID:         r.ID,
			CampaignID: uuid.UUID(r.CampaignID.Bytes).String(),
			OldStatus:  oldStatus,
			NewStatus:  string(r.NewStatus),
			Reason:     r.Reason.String,
			CreatedAt:  r.CreatedAt.Time.Format(time.RFC3339),
		}
	}
	return res, total, nil
}

// UpdateCampaignPacing changes manual pacing mode and propagates the update to the hot path via outbox.
func (s *Service) UpdateCampaignPacing(ctx context.Context, campaignID uuid.UUID, newMode string) (CampaignDTO, error) {
	var pacing db.PacingModeType
	switch newMode {
	case "ASAP":
		pacing = db.PacingModeTypeASAP
	case "EVEN":
		pacing = db.PacingModeTypeEVEN
	default:
		return CampaignDTO{}, fmt.Errorf("invalid pacing mode: %s", newMode)
	}

	var updatedCamp db.Campaign
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)

		camp, err := q.GetCampaignForUpdate(ctx, ads.ToUUID(campaignID))
		if err != nil {
			return fmt.Errorf("campaign not found: %w", err)
		}

		updatedCamp, err = q.UpdateCampaignPacing(ctx, db.UpdateCampaignPacingParams{
			ID:         ads.ToUUID(campaignID),
			PacingMode: pacing,
		})
		if err != nil {
			return fmt.Errorf("failed to update campaign pacing: %w", err)
		}

		var uid uuid.UUID
		if u, ok := GetUser(ctx); ok {
			uid = u.UserID
		}

		s.AuditLog(ctx, q, uid, "UPDATE_CAMPAIGN_PACING", "campaign", &campaignID, map[string]any{
			"old_pacing_mode": string(camp.PacingMode),
			"new_pacing_mode": string(pacing),
		}, nil)

		payloadBytes, err := json.Marshal(map[string]any{
			"campaign_id": campaignID.String(),
			"pacing_mode": string(pacing),
		})
		if err != nil {
			return fmt.Errorf("failed to marshal outbox payload: %w", err)
		}

		_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
			EventType: "UPDATE_CAMPAIGN_PACING",
			Payload:   payloadBytes,
		})
		if err != nil {
			return fmt.Errorf("failed to create outbox event: %w", err)
		}

		return nil
	})

	if err != nil {
		return CampaignDTO{}, err
	}

	return toCampaignDTO(updatedCamp), nil
}
