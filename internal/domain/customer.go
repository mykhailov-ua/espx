package domain

import (
	"context"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type Customer struct {
	ID       uuid.UUID
	Name     string
	Balance  decimal.Decimal
	Currency string
}

type CustomerRepository interface {
	GetByID(ctx context.Context, id uuid.UUID) (*Customer, error)
	UpdateBalance(ctx context.Context, id uuid.UUID, amount decimal.Decimal) error
}
