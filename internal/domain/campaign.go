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

type Campaign struct {
	ID           uuid.UUID
	CustomerID   uuid.UUID
	Name         string
	BudgetLimit  float64
	CurrentSpend float64
	Status       CampaignStatus
}

type CampaignRepository interface {
	GetByID(ctx context.Context, id uuid.UUID) (*Campaign, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status CampaignStatus) error
	UpdateSpend(ctx context.Context, id uuid.UUID, amount float64) error
	ListActive(ctx context.Context) ([]*Campaign, error)
}

type CampaignRegistry interface {
	Exists(id uuid.UUID) bool
	Add(id, customerID uuid.UUID)
	GetCustomerID(id uuid.UUID) (uuid.UUID, bool)
	Sync(ctx context.Context) (int, error)
	StartSync(ctx context.Context, interval time.Duration)
	Wait()
}
