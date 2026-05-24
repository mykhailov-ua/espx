package domain

import (
	"context"
	"github.com/google/uuid"
)

type Customer struct {
	ID       uuid.UUID
	Name     string
	Balance  int64
	Currency string
}

type CustomerRepository interface {
	GetByID(ctx context.Context, id uuid.UUID) (*Customer, error)
	UpdateBalance(ctx context.Context, id uuid.UUID, amount int64, txID string) error
}
