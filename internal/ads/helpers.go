package ads

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/shopspring/decimal"
)

func ToUUID(u uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: u, Valid: true}
}

const MicroUnitFactor = 1_000_000

var pow10 = [...]int64{
	1,                  // 10^0
	10,                 // 10^1
	100,                // 10^2
	1000,               // 10^3
	10000,              // 10^4
	100000,             // 10^5
	1000000,            // 10^6
	10000000,           // 10^7
	100000000,          // 10^8
	1000000000,         // 10^9
	10000000000,        // 10^10
	100000000000,       // 10^11
	1000000000000,      // 10^12
	10000000000000,     // 10^13
	100000000000000,    // 10^14
	1000000000000000,   // 10^15
	10000000000000000,  // 10^16
	100000000000000000, // 10^17
	1000000000000000000,// 10^18
}

func DecimalToMicro(d decimal.Decimal) int64 {
	val := d.CoefficientInt64()
	exp := d.Exponent()
	shift := exp + 6
	if shift >= 0 && shift < int32(len(pow10)) {
		return val * pow10[shift]
	} else if shift < 0 && -shift < int32(len(pow10)) {
		return val / pow10[-shift]
	}
	// Fallback to slow path if coefficient exceeds int64
	return d.Mul(decimal.NewFromInt(MicroUnitFactor)).IntPart()
}

func MicroToDecimal(m int64) decimal.Decimal {
	return decimal.NewFromInt(m).Div(decimal.NewFromInt(MicroUnitFactor))
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
	row, err := r.queries.GetCampaignFull(ctx, pgtype.UUID{Bytes: id, Valid: true})
	if err != nil {
		return nil, err
	}

	loc, _ := time.LoadLocation(row.Timezone)
	if loc == nil {
		loc = time.UTC
	}

	return &domain.Campaign{
		ID:              id,
		CustomerID:      uuid.UUID(row.CustomerID.Bytes),
		BudgetLimit:     FromNumeric(row.BudgetLimit),
		CurrentSpend:    FromNumeric(row.CurrentSpend),
		Status:          domain.CampaignStatus(row.Status),
		PacingMode:      domain.PacingMode(row.PacingMode),
		DailyBudget:     FromNumeric(row.DailyBudget),
		Timezone:        row.Timezone,
		Location:        loc,
		FreqLimit:       row.FreqLimit.Int32,
		FreqWindow:      row.FreqWindow.Int32,
		TargetCountries: row.TargetCountries,
	}, nil
}

func (r *CampaignRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.CampaignStatus) error {
	_, err := r.queries.UpdateCampaignStatus(ctx, db.UpdateCampaignStatusParams{
		ID:     pgtype.UUID{Bytes: id, Valid: true},
		Status: db.CampaignStatusType(status),
	})
	return err
}

func (r *CampaignRepo) UpdateSpend(ctx context.Context, id uuid.UUID, amount decimal.Decimal) error {
	return r.queries.UpdateCampaignSpend(ctx, db.UpdateCampaignSpendParams{
		ID:           pgtype.UUID{Bytes: id, Valid: true},
		CurrentSpend: ToNumeric(amount),
	})
}

func (r *CampaignRepo) ListActive(ctx context.Context) ([]*domain.Campaign, error) {
	rows, err := r.queries.ListActiveCampaigns(ctx)
	if err != nil {
		return nil, err
	}

	campaigns := make([]*domain.Campaign, len(rows))
	for i, row := range rows {
		loc, _ := time.LoadLocation(row.Timezone)
		if loc == nil {
			loc = time.UTC
		}

		campaigns[i] = &domain.Campaign{
			ID:              uuid.UUID(row.ID.Bytes),
			CustomerID:      uuid.UUID(row.CustomerID.Bytes),
			Name:            row.Name,
			BudgetLimit:     FromNumeric(row.BudgetLimit),
			CurrentSpend:    FromNumeric(row.CurrentSpend),
			Status:          domain.CampaignStatus(row.Status),
			PacingMode:      domain.PacingMode(row.PacingMode),
			DailyBudget:     FromNumeric(row.DailyBudget),
			Timezone:        row.Timezone,
			Location:        loc,
			FreqLimit:       row.FreqLimit.Int32,
			FreqWindow:      row.FreqWindow.Int32,
			TargetCountries: row.TargetCountries,
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

	return &domain.Customer{
		ID:       id,
		Name:     row.Name,
		Balance:  FromNumeric(row.Balance),
		Currency: row.Currency,
	}, nil
}

func (r *CustomerRepo) UpdateBalance(ctx context.Context, id uuid.UUID, amount decimal.Decimal) error {
	return r.queries.UpdateCustomerBalance(ctx, db.UpdateCustomerBalanceParams{
		ID:      pgtype.UUID{Bytes: id, Valid: true},
		Balance: ToNumeric(amount),
	})
}
