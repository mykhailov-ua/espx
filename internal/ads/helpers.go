package ads

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/shopspring/decimal"
)

func ToUUID(u uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: u, Valid: true}
}

func ToNumeric(d decimal.Decimal) pgtype.Numeric {
	n := pgtype.Numeric{}
	if err := n.Scan(d.StringFixed(2)); err != nil {
		panic(fmt.Sprintf("failed to scan decimal to numeric: %v", err))
	}
	return n
}

func FromNumeric(n pgtype.Numeric) decimal.Decimal {
	f, _ := n.Float64Value()
	if !f.Valid {
		return decimal.Zero
	}
	return decimal.NewFromFloat(f.Float64)
}

type CampaignRepo struct {
	queries db.Querier
}

func NewCampaignRepo(queries db.Querier) *CampaignRepo {
	return &CampaignRepo{queries: queries}
}

func (r *CampaignRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Campaign, error) {
	row, err := r.queries.GetCampaignBudget(ctx, pgtype.UUID{Bytes: id, Valid: true})
	if err != nil {
		return nil, err
	}

	limit, _ := row.BudgetLimit.Float64Value()
	spend, _ := row.CurrentSpend.Float64Value()

	return &domain.Campaign{
		ID:           id,
		CustomerID:   uuid.UUID(row.CustomerID.Bytes),
		BudgetLimit:  limit.Float64,
		CurrentSpend: spend.Float64,
		Status:       domain.CampaignStatus(row.Status),
	}, nil
}

func (r *CampaignRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.CampaignStatus) error {
	return nil
}

func (r *CampaignRepo) UpdateSpend(ctx context.Context, id uuid.UUID, amount float64) error {
	num := pgtype.Numeric{}
	if err := num.Scan(fmt.Sprintf("%.2f", amount)); err != nil {
		return err
	}
	return r.queries.UpdateCampaignSpend(ctx, db.UpdateCampaignSpendParams{
		ID:           pgtype.UUID{Bytes: id, Valid: true},
		CurrentSpend: num,
	})
}

func (r *CampaignRepo) ListActive(ctx context.Context) ([]*domain.Campaign, error) {
	rows, err := r.queries.ListActiveCampaigns(ctx)
	if err != nil {
		return nil, err
	}

	campaigns := make([]*domain.Campaign, len(rows))
	for i, row := range rows {
		limit, _ := row.BudgetLimit.Float64Value()
		spend, _ := row.CurrentSpend.Float64Value()

		campaigns[i] = &domain.Campaign{
			ID:           uuid.UUID(row.ID.Bytes),
			CustomerID:   uuid.UUID(row.CustomerID.Bytes),
			Name:         row.Name,
			BudgetLimit:  limit.Float64,
			CurrentSpend: spend.Float64,
			Status:       domain.CampaignStatus(row.Status),
		}
	}
	return campaigns, nil
}

type CustomerRepo struct {
	queries db.Querier
}

func NewCustomerRepo(queries db.Querier) *CustomerRepo {
	return &CustomerRepo{queries: queries}
}

func (r *CustomerRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Customer, error) {
	row, err := r.queries.GetCustomerByID(ctx, pgtype.UUID{Bytes: id, Valid: true})
	if err != nil {
		return nil, err
	}

	balance, _ := row.Balance.Float64Value()

	return &domain.Customer{
		ID:       id,
		Name:     row.Name,
		Balance:  balance.Float64,
		Currency: row.Currency,
	}, nil
}

func (r *CustomerRepo) UpdateBalance(ctx context.Context, id uuid.UUID, amount float64) error {
	num := pgtype.Numeric{}
	if err := num.Scan(fmt.Sprintf("%.2f", amount)); err != nil {
		return err
	}
	return r.queries.UpdateCustomerBalance(ctx, db.UpdateCustomerBalanceParams{
		ID:      pgtype.UUID{Bytes: id, Valid: true},
		Balance: num,
	})
}
