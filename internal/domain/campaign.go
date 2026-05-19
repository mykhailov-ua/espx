package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
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
	ID              uuid.UUID
	CustomerID      uuid.UUID
	Name            string
	BudgetLimit     decimal.Decimal
	CurrentSpend    decimal.Decimal
	Status          CampaignStatus
	PacingMode      PacingMode
	DailyBudget     decimal.Decimal
	Timezone        string
	Location        *time.Location
	FreqLimit       int32
	FreqWindow      int32
	TargetCountries []string
}

type CampaignRepository interface {
	GetByID(ctx context.Context, id uuid.UUID) (*Campaign, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status CampaignStatus) error
	UpdateSpend(ctx context.Context, id uuid.UUID, amount decimal.Decimal) error
	ListActive(ctx context.Context) ([]*Campaign, error)
}

type CampaignRegistry interface {
	Exists(id uuid.UUID) bool
	Add(id, customerID uuid.UUID, pacingMode PacingMode, dailyBudget decimal.Decimal, timezone string, freqLimit, freqWindow int32, targetCountries []string)
	GetCustomerID(id uuid.UUID) (uuid.UUID, bool)
	GetCampaign(id uuid.UUID) (*Campaign, bool)
	Sync(ctx context.Context) (int, error)
	StartSync(ctx context.Context, interval time.Duration)
	Wait(ctx context.Context) error
}
