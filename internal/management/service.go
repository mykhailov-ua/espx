package management

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/metrics"
	"github.com/redis/go-redis/v9"
)

type Service struct {
	pool     *pgxpool.Pool
	rdbs     []redis.UniversalClient
	sharder  ads.Sharder
	cfg      *config.Config
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	locCache sync.Map
}

func NewService(pool *pgxpool.Pool, rdbs []redis.UniversalClient, sharder ads.Sharder, cfg *config.Config) *Service {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		pool:    pool,
		rdbs:    rdbs,
		sharder: sharder,
		cfg:     cfg,
		ctx:     ctx,
		cancel:  cancel,
	}
	s.wg.Add(3)
	w := NewOutboxWorker(s)
	go func() {
		defer s.wg.Done()
		w.Start(ctx, 20*time.Millisecond)
	}()
	dw := NewCampaignDrainWorker(s)
	go func() {
		defer s.wg.Done()
		dw.Start(ctx, 20*time.Millisecond)
	}()
	csw := NewCreditScoringWorker(s)
	go func() {
		defer s.wg.Done()
		csw.Start(ctx, 24*time.Hour)
	}()
	return s
}

// StartReconWorker launches the financial reconciliation cold-path worker.
// The worker only ever looks at data at least two hours old. This hard guarantee
// removes all race conditions between reconciliation adjustments and the hot
// settlement path (SyncWorker + Processor pool). Interval is typically 15-30m.
func (s *Service) StartReconWorker(rdb redis.UniversalClient, interval time.Duration) {
	s.wg.Add(1)
	rw := NewReconWorker(s.pool, rdb, interval)
	go func() {
		defer s.wg.Done()
		rw.Start(s.ctx)
	}()
}

func (s *Service) GetPool() *pgxpool.Pool {
	return s.pool
}

func (s *Service) Close() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

// StartPacingController spawns the closed-loop pacing feedback worker in a background goroutine.
func (s *Service) StartPacingController(syncWorkers []*ads.SyncWorker, interval time.Duration) {
	// Periodic pacing adjustment enables real-time rate regulation across active campaigns.
	s.wg.Add(1)
	w := NewPacingControllerWorker(s, syncWorkers)
	go func() {
		defer s.wg.Done()
		w.Start(s.ctx, interval)
	}()
}

func (s *Service) GetCampaign(ctx context.Context, id uuid.UUID) (db.Campaign, error) {
	return db.New(s.pool).GetCampaignFull(ctx, ads.ToUUID(id))
}

func (s *Service) CreateCustomer(ctx context.Context, id uuid.UUID, name string, balance int64, currency string) error {
	_, err := db.New(s.pool).CreateCustomer(ctx, db.CreateCustomerParams{
		ID:       ads.ToUUID(id),
		Name:     name,
		Balance:  balance,
		Currency: currency,
	})
	if err == nil {
		s.AuditLog(ctx, nil, uuid.Nil, "CREATE_CUSTOMER", "customer", &id, map[string]any{"name": name, "balance": balance}, nil)
	}
	return err
}

func (s *Service) GenerateIdempotencyHash(customerID uuid.UUID, params any) string {
	b, _ := json.Marshal(params)
	h := sha256.New()
	h.Write([]byte(customerID.String()))
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

// TopUpBalance executes an atomic deposit into a customer ledger.
// It verifies idempotency against existing hashes within the same transaction to prevent double-crediting during network retries.
func (s *Service) TopUpBalance(ctx context.Context, customerID uuid.UUID, amount int64, idempotencyKey string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		_, err := q.GetLedgerByHashForUpdate(ctx, pgtype.Text{String: idempotencyKey, Valid: true})
		if err == nil {
			return nil
		}
		_, err = q.UpdateCustomerBalanceManagement(ctx, db.UpdateCustomerBalanceManagementParams{
			ID:      ads.ToUUID(customerID),
			Balance: amount,
		})
		if err != nil {
			return fmt.Errorf("failed to update balance: %w", err)
		}
		_, err = q.CreateLedgerEntry(ctx, db.CreateLedgerEntryParams{
			CustomerID:      ads.ToUUID(customerID),
			Amount:          amount,
			Type:            db.LedgerTypeTOPUP,
			IdempotencyHash: pgtype.Text{String: idempotencyKey, Valid: true},
		})
		if err == nil {
			metrics.BalanceTopupsTotal.WithLabelValues("USD").Add(float64(amount) / ads.MicroUnitFactor)
			s.AuditLog(ctx, q, uuid.Nil, "TOPUP_BALANCE", "customer", &customerID, map[string]any{"amount": amount}, map[string]any{"idempotency_key": idempotencyKey})
		}
		return err
	})
}

// CreateCampaign validates customer solvency and freezes the initial campaign budget within a single ACID transaction.
// The budget limit is subsequently synchronized to the sharded Redis pool to enable low-latency pacing evaluation at the edge.
func (s *Service) CreateCampaign(ctx context.Context, customerID uuid.UUID, brandID *uuid.UUID, name string, budgetLimit int64, pacingMode db.PacingModeType, dailyBudget int64, timezone string, freqLimit, freqWindow int32, targetCountries []string, idempotencyKey string) (uuid.UUID, error) {
	campaignID, _ := uuid.NewV7()
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		if existing, err := q.GetLedgerByHashForUpdate(ctx, pgtype.Text{String: idempotencyKey, Valid: true}); err == nil {
			if existing.CampaignID.Valid {
				campaignID = uuid.UUID(existing.CampaignID.Bytes)
				return nil
			}
		}
		cust, err := q.GetCustomerForUpdate(ctx, ads.ToUUID(customerID))
		if err != nil {
			return fmt.Errorf("customer not found: %w", err)
		}
		availableBalance := cust.Balance + cust.AllowedOverdraft
		if availableBalance < budgetLimit {
			return fmt.Errorf("insufficient balance")
		}

		var brandIDParam pgtype.UUID
		brandFcapKey := "fcap:c:" + campaignID.String()
		if brandID != nil {
			brand, err := q.GetBrand(ctx, ads.ToUUID(*brandID))
			if err != nil {
				return fmt.Errorf("brand not found: %w", err)
			}
			if uuid.UUID(brand.CustomerID.Bytes) != customerID {
				return fmt.Errorf("brand belongs to another customer")
			}
			brandIDParam = ads.ToUUID(*brandID)
			brandFcapKey = "fcap:b:" + brandID.String()
		}

		_, err = q.UpdateCustomerBalanceManagement(ctx, db.UpdateCustomerBalanceManagementParams{
			ID:      ads.ToUUID(customerID),
			Balance: -budgetLimit,
		})
		if err != nil {
			return err
		}
		_, err = q.CreateCampaign(ctx, db.CreateCampaignParams{
			ID:              ads.ToUUID(campaignID),
			Name:            name,
			BudgetLimit:     budgetLimit,
			Status:          db.CampaignStatusTypeACTIVE,
			CustomerID:      ads.ToUUID(customerID),
			PacingMode:      pacingMode,
			DailyBudget:     dailyBudget,
			Timezone:        timezone,
			FreqLimit:       pgtype.Int4{Int32: freqLimit, Valid: true},
			FreqWindow:      pgtype.Int4{Int32: freqWindow, Valid: true},
			TargetCountries: targetCountries,
			BrandID:         brandIDParam,
			BrandFcapKey:    brandFcapKey,
		})
		if err != nil {
			return err
		}
		_, err = q.CreateLedgerEntry(ctx, db.CreateLedgerEntryParams{
			CustomerID:      ads.ToUUID(customerID),
			CampaignID:      ads.ToUUID(campaignID),
			Amount:          budgetLimit,
			Type:            db.LedgerTypeFREEZE,
			IdempotencyHash: pgtype.Text{String: idempotencyKey, Valid: true},
		})
		if err != nil {
			return err
		}
		err = q.CreateStatusHistory(ctx, db.CreateStatusHistoryParams{
			CampaignID: ads.ToUUID(campaignID),
			NewStatus:  db.CampaignStatusTypeACTIVE,
			Reason:     pgtype.Text{String: "db.Campaign Creation", Valid: true},
		})
		if err == nil {
			s.AuditLog(ctx, q, uuid.Nil, "CREATE_CAMPAIGN", "campaign", &campaignID, map[string]any{
				"name":             name,
				"budget_limit":     budgetLimit,
				"customer_id":      customerID,
				"pacing_mode":      pacingMode,
				"daily_budget":     dailyBudget,
				"timezone":         timezone,
				"freq_limit":       freqLimit,
				"freq_window":      freqWindow,
				"target_countries": targetCountries,
			}, map[string]any{"idempotency_key": idempotencyKey})
			payloadBytes, _ := json.Marshal(CampaignPayload{CampaignID: campaignID.String(), BudgetLimit: budgetLimit})
			_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{EventType: "CREATE_CAMPAIGN", Payload: payloadBytes})
		}
		return err
	})
	return campaignID, err
}

// CancelCampaign transitions a campaign through a two-stage draining lifecycle to ensure inflight ad impressions complete before final budget reconciliation.
// Remaining funds are refunded to the customer balance minus a configured cancellation fee.
func (s *Service) CancelCampaign(ctx context.Context, campaignID uuid.UUID, reason string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		camp, err := q.GetCampaignFull(ctx, ads.ToUUID(campaignID))
		if err != nil {
			return err
		}
		if camp.Status == db.CampaignStatusTypeDELETED || camp.Status == db.CampaignStatusTypeDRAINING {
			return nil
		}
		_, err = q.UpdateCampaignStatus(ctx, db.UpdateCampaignStatusParams{
			ID:     ads.ToUUID(campaignID),
			Status: db.CampaignStatusTypeDRAINING,
		})
		if err != nil {
			return err
		}
		err = q.CreateStatusHistory(ctx, db.CreateStatusHistoryParams{
			CampaignID: ads.ToUUID(campaignID),
			OldStatus:  db.NullCampaignStatusType{CampaignStatusType: camp.Status, Valid: true},
			NewStatus:  db.CampaignStatusTypeDRAINING,
			Reason:     pgtype.Text{String: reason, Valid: true},
		})
		if err == nil {
			payloadBytes, _ := json.Marshal(CampaignPayload{CampaignID: campaignID.String()})
			_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{EventType: "CANCEL_CAMPAIGN", Payload: payloadBytes})
		}
		return err
	})
}

func (s *Service) FinalizeCancelledCampaign(ctx context.Context, campaignID uuid.UUID, reason string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		var camp db.Campaign
		err := tx.QueryRow(ctx, `
			SELECT status, budget_limit, current_spend, customer_id 
			FROM campaigns 
			WHERE id = $1 
			FOR UPDATE`, ads.ToUUID(campaignID)).Scan(&camp.Status, &camp.BudgetLimit, &camp.CurrentSpend, &camp.CustomerID)
		if err != nil {
			return err
		}
		if camp.Status != db.CampaignStatusTypeDRAINING {
			return nil
		}
		totalBudget := camp.BudgetLimit
		currentSpend := camp.CurrentSpend
		remaining := totalBudget - currentSpend
		if remaining < 0 {
			remaining = 0
		}
		fee := int64(float64(remaining) * (s.cfg.Management.CancellationFeePercent / 100.0))
		refund := remaining - fee
		if refund > 0 {
			_, err = q.UpdateCustomerBalanceManagement(ctx, db.UpdateCustomerBalanceManagementParams{
				ID:      camp.CustomerID,
				Balance: refund,
			})
			if err != nil {
				return err
			}
			_, err = q.CreateLedgerEntry(ctx, db.CreateLedgerEntryParams{
				CustomerID: camp.CustomerID,
				CampaignID: ads.ToUUID(campaignID),
				Amount:     refund,
				Type:       db.LedgerTypeRELEASE,
			})
			if err != nil {
				return err
			}
		}
		if fee > 0 {
			_, err = q.CreateLedgerEntry(ctx, db.CreateLedgerEntryParams{
				CustomerID: camp.CustomerID,
				CampaignID: ads.ToUUID(campaignID),
				Amount:     fee,
				Type:       db.LedgerTypeFEE,
			})
			if err != nil {
				return err
			}
			metrics.CommissionsCollectedTotal.Add(float64(fee) / ads.MicroUnitFactor)
		}
		err = q.SoftDeleteCampaign(ctx, ads.ToUUID(campaignID))
		if err != nil {
			return err
		}
		err = q.CreateStatusHistory(ctx, db.CreateStatusHistoryParams{
			CampaignID: ads.ToUUID(campaignID),
			OldStatus:  db.NullCampaignStatusType{CampaignStatusType: db.CampaignStatusTypeDRAINING, Valid: true},
			NewStatus:  db.CampaignStatusTypeDELETED,
			Reason:     pgtype.Text{String: "Finalized", Valid: true},
		})
		if err != nil {
			return err
		}
		s.AuditLog(ctx, q, uuid.Nil, "CANCEL_CAMPAIGN", "campaign", &campaignID, map[string]any{"reason": reason}, nil)
		return nil
	})
}

func (s *Service) getRDB(campaignID uuid.UUID) redis.UniversalClient {
	if len(s.rdbs) <= 1 {
		return s.rdbs[0]
	}
	idx := s.sharder.GetShard(campaignID)
	return s.rdbs[idx%len(s.rdbs)]
}

func (s *Service) ListAuditLogs(ctx context.Context, limit, offset int32) ([]db.AdminAuditLog, error) {
	return db.New(s.pool).ListAuditLogs(ctx, db.ListAuditLogsParams{
		Limit:  limit,
		Offset: offset,
	})
}

// UpdateOverdraft updates a customer's allowed overdraft inside a database transaction and records audit trail.
func (s *Service) UpdateOverdraft(ctx context.Context, id uuid.UUID, newOverdraft, oldOverdraft int64) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		cust, err := q.GetCustomerForUpdate(ctx, ads.ToUUID(id))
		if err != nil {
			return fmt.Errorf("failed to fetch customer for overdraft update: %w", err)
		}

		if newOverdraft < oldOverdraft {
			availableLimit := cust.Balance + newOverdraft
			if availableLimit < 0 {
				camps, err := q.ListCampaigns(ctx, db.ListCampaignsParams{
					Limit:      10000,
					Offset:     0,
					CustomerID: ads.ToUUID(id),
					Status:     pgtype.Text{String: string(db.CampaignStatusTypeACTIVE), Valid: true},
				})
				if err != nil {
					return fmt.Errorf("failed to list active campaigns for overdraft decrease: %w", err)
				}

				for _, c := range camps {
					if availableLimit >= 0 {
						break
					}

					_, err = q.UpdateCampaignStatus(ctx, db.UpdateCampaignStatusParams{
						ID:     c.ID,
						Status: db.CampaignStatusTypePAUSED,
					})
					if err != nil {
						return fmt.Errorf("failed to pause campaign: %w", err)
					}

					err = q.CreateStatusHistory(ctx, db.CreateStatusHistoryParams{
						CampaignID: c.ID,
						OldStatus:  db.NullCampaignStatusType{CampaignStatusType: db.CampaignStatusTypeACTIVE, Valid: true},
						NewStatus:  db.CampaignStatusTypePAUSED,
						Reason:     pgtype.Text{String: "Overdraft reduced, campaign suspended", Valid: true},
					})
					if err != nil {
						return fmt.Errorf("failed to write status history: %w", err)
					}

					budgetLimit := c.BudgetLimit
					currentSpend := c.CurrentSpend
					remaining := budgetLimit - currentSpend
					if remaining < 0 {
						remaining = 0
					}

					if remaining > 0 {
						_, err = q.UpdateCustomerBalanceManagement(ctx, db.UpdateCustomerBalanceManagementParams{
							ID:      ads.ToUUID(id),
							Balance: remaining,
						})
						if err != nil {
							return fmt.Errorf("failed to refund balance for suspended campaign: %w", err)
						}

						_, err = q.CreateLedgerEntry(ctx, db.CreateLedgerEntryParams{
							CustomerID: ads.ToUUID(id),
							CampaignID: c.ID,
							Amount:     remaining,
							Type:       db.LedgerTypeRELEASE,
						})
						if err != nil {
							return fmt.Errorf("failed to record release ledger entry: %w", err)
						}

						availableLimit = availableLimit + remaining
					}

					payloadBytes, _ := json.Marshal(CampaignPayload{CampaignID: uuid.UUID(c.ID.Bytes).String()})
					_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
						EventType: "CANCEL_CAMPAIGN",
						Payload:   payloadBytes,
					})
					if err != nil {
						return fmt.Errorf("failed to emit outbox event for paused campaign: %w", err)
					}

					campID := uuid.UUID(c.ID.Bytes)
					s.AuditLog(ctx, q, uuid.Nil, "SUSPEND_CAMPAIGN", "campaign", &campID, map[string]any{"reason": "overdraft_reduced"}, nil)
				}
			}
		}

		_, err = q.UpdateCustomerOverdraft(ctx, db.UpdateCustomerOverdraftParams{
			ID:               ads.ToUUID(id),
			AllowedOverdraft: newOverdraft,
		})
		if err != nil {
			return err
		}

		s.AuditLog(ctx, q, uuid.Nil, "UPDATE_CUSTOMER_OVERDRAFT", "customer", &id, map[string]any{
			"old_overdraft": fmt.Sprintf("%.2f", float64(oldOverdraft)/ads.MicroUnitFactor),
			"new_overdraft": fmt.Sprintf("%.2f", float64(newOverdraft)/ads.MicroUnitFactor),
		}, nil)
		return nil
	})
}
