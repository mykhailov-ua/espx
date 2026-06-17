package management

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/internal/config"
	"espx/internal/metrics"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// Service coordinates management business logic, background workers, and hot-path propagation via outbox.
type Service struct {
	pool     *pgxpool.Pool
	rdbs     []redis.UniversalClient
	sharder  ads.Sharder
	cfg      *config.Config
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	workerMu sync.Mutex
	closed   atomic.Bool
	locCache sync.Map
}

// StartBackgroundWorker launches an auxiliary goroutine tracked for graceful shutdown.
func (s *Service) StartBackgroundWorker(fn func()) {
	s.startWorker(fn)
}

// startWorker launches a background goroutine tracked for graceful shutdown.
func (s *Service) startWorker(fn func()) {
	s.workerMu.Lock()
	if s.closed.Load() {
		s.workerMu.Unlock()
		return
	}
	s.wg.Add(1)
	s.workerMu.Unlock()

	go func() {
		defer s.wg.Done()
		fn()
	}()
}

// NewService constructs the management service and starts core background workers.
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
	s.startWorker(func() {
		NewOutboxWorker(s).Start(ctx, 20*time.Millisecond)
	})
	s.startWorker(func() {
		NewCampaignDrainWorker(s).Start(ctx, 20*time.Millisecond)
	})
	s.startWorker(func() {
		NewCreditScoringWorker(s).Start(ctx, 24*time.Hour)
	})
	s.startWorker(func() {
		NewScheduleWorker(s).Start(ctx)
	})
	s.startWorker(func() {
		s.RunSystemStateSyncer(ctx)
	})
	return s
}

// StartReconWorker starts periodic ledger reconciliation on the given interval.
func (s *Service) StartReconWorker(interval time.Duration) {
	s.startWorker(func() {
		NewReconWorker(s, interval).Start(s.ctx)
	})
}

// StartAuditCleaner deletes audit rows older than the configured retention window.
func (s *Service) StartAuditCleaner(retention Days) {
	s.startWorker(func() {
		s.RunAuditCleaner(s.ctx, retention)
	})
}

// GetPool exposes the Postgres pool for tests and auxiliary workers.
func (s *Service) GetPool() *pgxpool.Pool {
	return s.pool
}

// Close cancels background workers and waits for them to exit.
func (s *Service) Close() {
	s.closed.Store(true)
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

// StartPacingController starts the closed-loop pacing worker with budget sync dependencies.
func (s *Service) StartPacingController(syncWorkers []*ads.SyncWorker, interval time.Duration) {
	s.startWorker(func() {
		NewPacingControllerWorker(s, syncWorkers).Start(s.ctx, interval)
	})
}

// GetCampaign loads the full campaign row for internal authorization and lifecycle checks.
func (s *Service) GetCampaign(ctx context.Context, id uuid.UUID) (db.Campaign, error) {
	return db.New(s.pool).GetCampaignFull(ctx, ads.ToUUID(id))
}

// CreateCustomer registers a new billing account with an optional opening balance.
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

// GenerateIdempotencyHash derives a stable key from customer identity and request payload for safe retries.
func (s *Service) GenerateIdempotencyHash(customerID uuid.UUID, params any) string {
	b, _ := json.Marshal(params)
	h := sha256.New()
	h.Write([]byte(customerID.String()))
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

// TopUpBalance credits a customer account idempotently and records the ledger entry.
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

// CancelCampaign marks a campaign draining so the hot path can finish in-flight bids before refund.
func (s *Service) CancelCampaign(ctx context.Context, campaignID uuid.UUID, reason string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		camp, err := q.GetCampaignForUpdate(ctx, ads.ToUUID(campaignID))
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

// FinalizeCancelledCampaign completes refund and deletion for one draining campaign under row lock.
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
		return s.finalizeDrainingCampaign(ctx, q, campaignID, camp, reason)
	})
}

// finalizeDrainingCampaign releases remaining budget, collects fees, and soft-deletes a draining campaign.
func (s *Service) finalizeDrainingCampaign(ctx context.Context, q db.Querier, campaignID uuid.UUID, camp db.Campaign, reason string) error {
	if camp.Status != db.CampaignStatusTypeDRAINING {
		return nil
	}
	totalBudget := camp.BudgetLimit
	currentSpend := camp.CurrentSpend
	remaining := totalBudget - currentSpend
	if remaining < 0 {
		remaining = 0
	}
	feePercent := 0.0
	if s.cfg != nil {
		feePercent = s.cfg.Management.CancellationFeePercent
	}
	fee := int64(float64(remaining) * (feePercent / 100.0))
	refund := remaining - fee
	if refund > 0 {
		_, err := q.UpdateCustomerBalanceManagement(ctx, db.UpdateCustomerBalanceManagementParams{
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
		_, err := q.CreateLedgerEntry(ctx, db.CreateLedgerEntryParams{
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
	if err := q.SoftDeleteCampaign(ctx, ads.ToUUID(campaignID)); err != nil {
		return err
	}
	if err := q.CreateStatusHistory(ctx, db.CreateStatusHistoryParams{
		CampaignID: ads.ToUUID(campaignID),
		OldStatus:  db.NullCampaignStatusType{CampaignStatusType: db.CampaignStatusTypeDRAINING, Valid: true},
		NewStatus:  db.CampaignStatusTypeDELETED,
		Reason:     pgtype.Text{String: "Finalized", Valid: true},
	}); err != nil {
		return err
	}
	s.AuditLog(ctx, q, uuid.Nil, "CANCEL_CAMPAIGN", "campaign", &campaignID, map[string]any{"reason": reason}, nil)
	return nil
}

// campaignUpdateChannel returns the Redis pubsub channel used to invalidate hot-path campaign caches.
func (s *Service) campaignUpdateChannel() string {
	if s.cfg != nil && s.cfg.CampaignUpdateChannel != "" {
		return s.cfg.CampaignUpdateChannel
	}
	return "campaigns:update"
}

// getRDB selects the Redis shard that owns a campaign's budget and settings keys.
func (s *Service) getRDB(campaignID uuid.UUID) redis.UniversalClient {
	if len(s.rdbs) == 0 {
		return nil
	}
	if len(s.rdbs) == 1 {
		return s.rdbs[0]
	}
	idx := s.sharder.GetShard(campaignID)
	return s.rdbs[idx%len(s.rdbs)]
}

// ListAuditLogs returns paginated admin audit entries for compliance review.
func (s *Service) ListAuditLogs(ctx context.Context, limit, offset int32) ([]db.AdminAuditLog, error) {
	return db.New(s.pool).ListAuditLogs(ctx, db.ListAuditLogsParams{
		Limit:  limit,
		Offset: offset,
	})
}

// UpdateOverdraft adjusts credit limits and suspends campaigns when reduced overdraft would overcommit balance.
func (s *Service) UpdateOverdraft(ctx context.Context, id uuid.UUID, newOverdraft int64) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		cust, err := q.GetCustomerForUpdate(ctx, ads.ToUUID(id))
		if err != nil {
			return fmt.Errorf("failed to fetch customer for overdraft update: %w", err)
		}

		prevOverdraft := cust.AllowedOverdraft
		if newOverdraft == prevOverdraft {
			return nil
		}

		if newOverdraft < prevOverdraft {
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

					locked, err := q.GetCampaignForUpdate(ctx, c.ID)
					if err != nil {
						return fmt.Errorf("failed to lock campaign for overdraft suspend: %w", err)
					}
					if locked.Status != db.CampaignStatusTypeACTIVE {
						continue
					}

					_, err = q.UpdateCampaignStatus(ctx, db.UpdateCampaignStatusParams{
						ID:     locked.ID,
						Status: db.CampaignStatusTypePAUSED,
					})
					if err != nil {
						return fmt.Errorf("failed to pause campaign: %w", err)
					}

					err = q.CreateStatusHistory(ctx, db.CreateStatusHistoryParams{
						CampaignID: locked.ID,
						OldStatus:  db.NullCampaignStatusType{CampaignStatusType: db.CampaignStatusTypeACTIVE, Valid: true},
						NewStatus:  db.CampaignStatusTypePAUSED,
						Reason:     pgtype.Text{String: "Overdraft reduced, campaign suspended", Valid: true},
					})
					if err != nil {
						return fmt.Errorf("failed to write status history: %w", err)
					}

					budgetLimit := locked.BudgetLimit
					currentSpend := locked.CurrentSpend
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
							CampaignID: locked.ID,
							Amount:     remaining,
							Type:       db.LedgerTypeRELEASE,
						})
						if err != nil {
							return fmt.Errorf("failed to record release ledger entry: %w", err)
						}

						availableLimit = availableLimit + remaining
					}

					payloadBytes, _ := json.Marshal(CampaignPayload{CampaignID: uuid.UUID(locked.ID.Bytes).String()})
					_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
						EventType: "PAUSE_CAMPAIGN",
						Payload:   payloadBytes,
					})
					if err != nil {
						return fmt.Errorf("failed to emit outbox event for paused campaign: %w", err)
					}

					campID := uuid.UUID(locked.ID.Bytes)
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
			"old_overdraft": fmt.Sprintf("%.2f", float64(prevOverdraft)/ads.MicroUnitFactor),
			"new_overdraft": fmt.Sprintf("%.2f", float64(newOverdraft)/ads.MicroUnitFactor),
		}, nil)
		return nil
	})
}
