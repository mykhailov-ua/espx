package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type CampaignStatus string

const (
	CampaignStatusActive    CampaignStatus = "ACTIVE"
	CampaignStatusPaused    CampaignStatus = "PAUSED"
	CampaignStatusExhausted CampaignStatus = "EXHAUSTED"
)

type PacingMode string

const (
	PacingModeAsap PacingMode = "ASAP"
	PacingModeEven PacingMode = "EVEN"
)

type Campaign struct {
	ID                  uuid.UUID
	IDStr               string
	IDStrAny            any
	CustomerID          uuid.UUID
	CustomerIDStr       string
	CustomerIDStrAny    any
	BrandID             *uuid.UUID
	BrandFcapKey        string
	Name                string
	BudgetLimit         int64
	CurrentSpend        int64
	Status              CampaignStatus
	PacingMode          PacingMode
	DailyBudget         int64
	DailyBudgetMicro    int64
	DailyBudgetMicroAny any
	Timezone            string
	Location            *time.Location
	FreqLimit           int32
	FreqLimitAny        any
	FreqWindow          int32
	FreqWindowAny       any
	TargetCountries     map[string]struct{}
}

type Brand struct {
	ID         uuid.UUID
	CustomerID uuid.UUID
	Name       string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type CampaignRepository interface {
	GetByID(ctx context.Context, id uuid.UUID) (*Campaign, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status CampaignStatus) error
	UpdateSpend(ctx context.Context, id uuid.UUID, amount int64, txID string) error
	ListActive(ctx context.Context) ([]*Campaign, error)
}

type CampaignRegistry interface {
	Exists(id uuid.UUID) bool
	Add(id, customerID uuid.UUID, brandID *uuid.UUID, brandFcapKey string, pacingMode PacingMode, dailyBudget int64, timezone string, freqLimit, freqWindow int32, targetCountries []string)
	GetCustomerID(id uuid.UUID) (uuid.UUID, bool)
	GetCampaign(id uuid.UUID) (*Campaign, bool)
	Sync(ctx context.Context) (int, error)
	StartSync(ctx context.Context, interval time.Duration)
	Wait(ctx context.Context) error
}
