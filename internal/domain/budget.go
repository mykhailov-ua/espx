package domain

import (
	"context"
	"github.com/google/uuid"
)

type BudgetManager interface {
	CheckAndSpend(ctx context.Context, customerID, campaignID uuid.UUID, clickID string, amount int64) (bool, error)
}
