package management

import (
	"fmt"
	"time"

	"espx/internal/ads/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// CampaignCreateSpec bundles validated inputs for atomic campaign creation with idempotency.
type CampaignCreateSpec struct {
	CustomerID      uuid.UUID
	BrandID         *uuid.UUID
	Name            string
	BudgetLimit     int64
	PacingMode      db.PacingModeType
	DailyBudget     int64
	Timezone        string
	FreqLimit       int32
	FreqWindow      int32
	TargetCountries []string
	StartAt         *time.Time
	EndAt           *time.Time
	DaypartHours    []int16
	TemplateID      *uuid.UUID
	IdempotencyKey  string
}

// CampaignTemplateDTO exposes reusable campaign presets to the admin API.
type CampaignTemplateDTO struct {
	ID              string   `json:"id"`
	CustomerID      string   `json:"customer_id"`
	Name            string   `json:"name"`
	BudgetLimit     string   `json:"budget_limit"`
	PacingMode      string   `json:"pacing_mode"`
	DailyBudget     string   `json:"daily_budget"`
	Timezone        string   `json:"timezone"`
	FreqLimit       int32    `json:"freq_limit"`
	FreqWindow      int32    `json:"freq_window"`
	TargetCountries []string `json:"target_countries"`
	BrandID         string   `json:"brand_id,omitempty"`
	DaypartHours    []int16  `json:"daypart_hours"`
	CreatedAt       string   `json:"created_at"`
	UpdatedAt       string   `json:"updated_at"`
}

// BrandCreativeDTO represents a weighted landing URL variant served for a brand.
type BrandCreativeDTO struct {
	ID         string `json:"id"`
	BrandID    string `json:"brand_id"`
	Name       string `json:"name"`
	LandingURL string `json:"landing_url"`
	Weight     int32  `json:"weight"`
	Status     string `json:"status"`
	CreatedAt  string `json:"created_at"`
	UpdatedAt  string `json:"updated_at"`
}

// validateDaypartHours rejects hour values outside the 0-23 delivery window range.
func validateDaypartHours(hours []int16) error {
	for _, h := range hours {
		if h < 0 || h > 23 {
			return fmt.Errorf("daypart hour must be 0-23, got %d", h)
		}
	}
	return nil
}

// validateSchedule ensures scheduled campaigns have a coherent start and end interval.
func validateSchedule(startAt, endAt *time.Time) error {
	if startAt != nil && endAt != nil && !endAt.After(*startAt) {
		return fmt.Errorf("end_at must be after start_at")
	}
	return nil
}

// countriesOrEmpty normalizes nil country slices to empty JSON arrays in API responses.
func countriesOrEmpty(c []string) []string {
	if c == nil {
		return []string{}
	}
	return c
}

// resolveScheduleStatus derives ACTIVE or PAUSED from whether now falls inside the campaign window.
func resolveScheduleStatus(now time.Time, startAt, endAt *time.Time) db.CampaignStatusType {
	if startAt != nil && now.Before(*startAt) {
		return db.CampaignStatusTypePAUSED
	}
	if endAt != nil && !now.Before(*endAt) {
		return db.CampaignStatusTypePAUSED
	}
	return db.CampaignStatusTypeACTIVE
}

// toTimestamptz converts optional Go times into pgx timestamptz values for database writes.
func toTimestamptz(t *time.Time) pgtype.Timestamptz {
	if t == nil {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: *t, Valid: true}
}

// templateToDTO maps a stored template row into the admin API representation.
func templateToDTO(t db.CampaignTemplate) CampaignTemplateDTO {
	countries := t.TargetCountries
	if countries == nil {
		countries = []string{}
	}
	hours := t.DaypartHours
	if hours == nil {
		hours = []int16{}
	}
	var brandID string
	if t.BrandID.Valid {
		brandID = uuid.UUID(t.BrandID.Bytes).String()
	}
	return CampaignTemplateDTO{
		ID:              uuid.UUID(t.ID.Bytes).String(),
		CustomerID:      uuid.UUID(t.CustomerID.Bytes).String(),
		Name:            t.Name,
		BudgetLimit:     formatMicro(t.BudgetLimit),
		PacingMode:      string(t.PacingMode),
		DailyBudget:     formatMicro(t.DailyBudget),
		Timezone:        t.Timezone,
		FreqLimit:       t.FreqLimit,
		FreqWindow:      t.FreqWindow,
		TargetCountries: countries,
		BrandID:         brandID,
		DaypartHours:    hours,
		CreatedAt:       t.CreatedAt.Time.Format(time.RFC3339),
		UpdatedAt:       t.UpdatedAt.Time.Format(time.RFC3339),
	}
}

// creativeToDTO maps a brand creative row into the admin API representation.
func creativeToDTO(c db.BrandCreative) BrandCreativeDTO {
	return BrandCreativeDTO{
		ID:         uuid.UUID(c.ID.Bytes).String(),
		BrandID:    uuid.UUID(c.BrandID.Bytes).String(),
		Name:       c.Name,
		LandingURL: c.LandingUrl,
		Weight:     c.Weight,
		Status:     c.Status,
		CreatedAt:  c.CreatedAt.Time.Format(time.RFC3339),
		UpdatedAt:  c.UpdatedAt.Time.Format(time.RFC3339),
	}
}
