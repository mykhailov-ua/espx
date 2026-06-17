package management

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// BrandDTO exposes advertiser brand metadata and frequency-cap settings to the admin API.
type BrandDTO struct {
	ID         string `json:"id"`
	CustomerID string `json:"customer_id"`
	Name       string `json:"name"`
	CreatedAt  string `json:"created_at"`
	UpdatedAt  string `json:"updated_at"`
	FreqLimit  int32  `json:"freq_limit"`
	FreqWindow int32  `json:"freq_window"`
}

// toBrandDTO maps a database brand row into the admin API representation.
func toBrandDTO(b db.AdvertiserBrand) BrandDTO {
	return BrandDTO{
		ID:         uuid.UUID(b.ID.Bytes).String(),
		CustomerID: uuid.UUID(b.CustomerID.Bytes).String(),
		Name:       b.Name,
		CreatedAt:  b.CreatedAt.Time.Format(time.RFC3339),
		UpdatedAt:  b.UpdatedAt.Time.Format(time.RFC3339),
		FreqLimit:  b.FreqLimit,
		FreqWindow: b.FreqWindow,
	}
}

// CreateBrand registers a new advertiser brand under an existing customer account.
func (s *Service) CreateBrand(ctx context.Context, customerID uuid.UUID, name string) (uuid.UUID, error) {
	brandID, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, err
	}

	q := db.New(s.pool)
	_, err = q.GetCustomerByID(ctx, ads.ToUUID(customerID))
	if err != nil {
		return uuid.Nil, fmt.Errorf("customer not found: %w", err)
	}

	_, err = q.CreateBrand(ctx, db.CreateBrandParams{
		ID:         ads.ToUUID(brandID),
		CustomerID: ads.ToUUID(customerID),
		Name:       name,
	})
	if err != nil {
		return uuid.Nil, err
	}

	return brandID, nil
}

// GetBrandDTO loads a single brand for admin display and access checks.
func (s *Service) GetBrandDTO(ctx context.Context, id uuid.UUID) (BrandDTO, error) {
	q := db.New(s.pool)
	b, err := q.GetBrand(ctx, ads.ToUUID(id))
	if err != nil {
		return BrandDTO{}, err
	}
	return toBrandDTO(b), nil
}

// ListBrandsByCustomer returns all brands owned by a customer for the admin UI.
func (s *Service) ListBrandsByCustomer(ctx context.Context, customerID uuid.UUID) ([]BrandDTO, error) {
	q := db.New(s.pool)
	rows, err := q.ListBrandsByCustomer(ctx, ads.ToUUID(customerID))
	if err != nil {
		return nil, err
	}

	res := make([]BrandDTO, len(rows))
	for i, r := range rows {
		res[i] = toBrandDTO(r)
	}
	return res, nil
}

// ConfigureBrandFcap updates brand-level frequency caps and notifies the hot path via outbox.
func (s *Service) ConfigureBrandFcap(ctx context.Context, brandID uuid.UUID, limit, window int32) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)

		brand, err := q.GetBrandForUpdate(ctx, ads.ToUUID(brandID))
		if err != nil {
			return fmt.Errorf("brand not found: %w", err)
		}

		err = q.ConfigureBrandFcap(ctx, db.ConfigureBrandFcapParams{
			ID:         ads.ToUUID(brandID),
			FreqLimit:  limit,
			FreqWindow: window,
		})
		if err != nil {
			return fmt.Errorf("failed to update brand fcap limits: %w", err)
		}

		payloadBytes, err := json.Marshal(map[string]any{
			"brand_id":    brandID.String(),
			"freq_limit":  limit,
			"freq_window": window,
		})
		if err != nil {
			return fmt.Errorf("failed to marshal outbox event payload: %w", err)
		}

		_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
			EventType: "CONFIGURE_BRAND_FCAP",
			Payload:   payloadBytes,
		})
		if err != nil {
			return fmt.Errorf("failed to create outbox event: %w", err)
		}

		s.AuditLog(ctx, q, uuid.Nil, "CONFIGURE_BRAND_FCAP", "brand", &brandID, map[string]any{
			"old_freq_limit":  brand.FreqLimit,
			"old_freq_window": brand.FreqWindow,
			"new_freq_limit":  limit,
			"new_freq_window": window,
		}, nil)

		return nil
	})
}
