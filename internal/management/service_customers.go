package management

import (
	"context"
	"fmt"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// CustomerDTO aggregates customer account data and spend stats for the admin API.
type CustomerDTO struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	Balance         string `json:"balance"`
	Currency        string `json:"currency"`
	ActiveCampaigns int64  `json:"active_campaigns"`
	TotalSpend      string `json:"total_spend"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`
}

// LedgerDTO exposes a single balance ledger entry for customer billing history.
type LedgerDTO struct {
	ID              int64  `json:"id"`
	CustomerID      string `json:"customer_id"`
	CampaignID      string `json:"campaign_id,omitempty"`
	Amount          string `json:"amount"`
	Type            string `json:"type"`
	IdempotencyHash string `json:"idempotency_hash,omitempty"`
	CreatedAt       string `json:"created_at"`
}

// formatMicro converts micro-unit balances to a two-decimal string for JSON responses.
func formatMicro(m int64) string {
	return fmt.Sprintf("%.2f", float64(m)/1_000_000.0)
}

// ListCustomers returns a paginated customer list enriched with campaign spend aggregates.
func (s *Service) ListCustomers(ctx context.Context, limit, offset int32) ([]CustomerDTO, int64, error) {
	q := db.New(s.pool)
	total, err := q.CountCustomers(ctx)
	if err != nil {
		return nil, 0, err
	}
	if total == 0 {
		return []CustomerDTO{}, 0, nil
	}

	rows, err := q.ListCustomers(ctx, db.ListCustomersParams{Limit: limit, Offset: offset})
	if err != nil {
		return nil, 0, err
	}

	var customerIDs []pgtype.UUID
	for _, r := range rows {
		customerIDs = append(customerIDs, r.ID)
	}

	stats, err := q.GetCustomerStats(ctx, customerIDs)
	if err != nil {
		return nil, 0, err
	}

	statsMap := make(map[uuid.UUID]db.GetCustomerStatsRow)
	for _, st := range stats {
		if st.CustomerID.Valid {
			statsMap[uuid.UUID(st.CustomerID.Bytes)] = st
		}
	}

	res := make([]CustomerDTO, len(rows))
	for i, r := range rows {
		uid := uuid.UUID(r.ID.Bytes)
		st := statsMap[uid]
		res[i] = CustomerDTO{
			ID:              uid.String(),
			Name:            r.Name,
			Balance:         formatMicro(r.Balance),
			Currency:        r.Currency,
			ActiveCampaigns: st.ActiveCampaigns,
			TotalSpend:      formatMicro(st.TotalSpend),
			CreatedAt:       r.CreatedAt.Time.Format(time.RFC3339),
			UpdatedAt:       r.UpdatedAt.Time.Format(time.RFC3339),
		}
	}

	return res, total, nil
}

// GetCustomerDTO loads one customer with aggregated stats for detail views.
func (s *Service) GetCustomerDTO(ctx context.Context, id uuid.UUID) (CustomerDTO, error) {
	q := db.New(s.pool)
	r, err := q.GetCustomerByID(ctx, ads.ToUUID(id))
	if err != nil {
		return CustomerDTO{}, err
	}

	stats, err := q.GetCustomerStats(ctx, []pgtype.UUID{r.ID})
	if err != nil {
		return CustomerDTO{}, err
	}

	var st db.GetCustomerStatsRow
	if len(stats) > 0 {
		st = stats[0]
	}

	return CustomerDTO{
		ID:              uuid.UUID(r.ID.Bytes).String(),
		Name:            r.Name,
		Balance:         formatMicro(r.Balance),
		Currency:        r.Currency,
		ActiveCampaigns: st.ActiveCampaigns,
		TotalSpend:      formatMicro(st.TotalSpend),
		CreatedAt:       r.CreatedAt.Time.Format(time.RFC3339),
		UpdatedAt:       r.UpdatedAt.Time.Format(time.RFC3339),
	}, nil
}

// ListCustomerLedger returns paginated ledger entries for a customer's billing history.
func (s *Service) ListCustomerLedger(ctx context.Context, customerID uuid.UUID, limit, offset int32) ([]LedgerDTO, int64, error) {
	q := db.New(s.pool)
	tid := ads.ToUUID(customerID)
	total, err := q.CountCustomerLedger(ctx, tid)
	if err != nil {
		return nil, 0, err
	}
	if total == 0 {
		return []LedgerDTO{}, 0, nil
	}

	rows, err := q.ListCustomerLedger(ctx, db.ListCustomerLedgerParams{
		CustomerID: tid,
		Limit:      limit,
		Offset:     offset,
	})
	if err != nil {
		return nil, 0, err
	}

	res := make([]LedgerDTO, len(rows))
	for i, r := range rows {
		var campID string
		if r.CampaignID.Valid {
			campID = uuid.UUID(r.CampaignID.Bytes).String()
		}
		res[i] = LedgerDTO{
			ID:              r.ID,
			CustomerID:      uuid.UUID(r.CustomerID.Bytes).String(),
			CampaignID:      campID,
			Amount:          formatMicro(r.Amount),
			Type:            string(r.Type),
			IdempotencyHash: r.IdempotencyHash.String,
			CreatedAt:       r.CreatedAt.Time.Format(time.RFC3339),
		}
	}
	return res, total, nil
}
