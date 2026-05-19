package domain

import (
	"context"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type BudgetManager interface {
	CheckAndSpend(ctx context.Context, customerID, campaignID uuid.UUID, clickID string, amount decimal.Decimal) (bool, error)
}
