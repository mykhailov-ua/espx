package management

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/metrics"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

type Service struct {
	pool    *pgxpool.Pool
	rdbs    []redis.UniversalClient
	sharder ads.Sharder
	cfg     *config.Config
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
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
	s.wg.Add(2)
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
	return s
}

func (s *Service) Close() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

func (s *Service) GetCampaign(ctx context.Context, id uuid.UUID) (db.Campaign, error) {
	return db.New(s.pool).GetCampaignFull(ctx, ads.ToUUID(id))
}

func (s *Service) CreateCustomer(ctx context.Context, id uuid.UUID, name string, balance decimal.Decimal, currency string) error {
	_, err := db.New(s.pool).CreateCustomer(ctx, db.CreateCustomerParams{
		ID:       ads.ToUUID(id),
		Name:     name,
		Balance:  ads.ToNumeric(balance),
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
func (s *Service) TopUpBalance(ctx context.Context, customerID uuid.UUID, amount decimal.Decimal, idempotencyKey string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		_, err := q.GetLedgerByHashForUpdate(ctx, pgtype.Text{String: idempotencyKey, Valid: true})
		if err == nil {
			return nil
		}
		_, err = q.UpdateCustomerBalanceManagement(ctx, db.UpdateCustomerBalanceManagementParams{
			ID:      ads.ToUUID(customerID),
			Balance: ads.ToNumeric(amount),
		})
		if err != nil {
			return fmt.Errorf("failed to update balance: %w", err)
		}
		_, err = q.CreateLedgerEntry(ctx, db.CreateLedgerEntryParams{
			CustomerID:      ads.ToUUID(customerID),
			Amount:          ads.ToNumeric(amount),
			Type:            db.LedgerTypeTOPUP,
			IdempotencyHash: pgtype.Text{String: idempotencyKey, Valid: true},
		})
		if err == nil {
			metrics.BalanceTopupsTotal.WithLabelValues("USD").Add(amount.InexactFloat64())
			s.AuditLog(ctx, q, uuid.Nil, "TOPUP_BALANCE", "customer", &customerID, map[string]any{"amount": amount}, map[string]any{"idempotency_key": idempotencyKey})
		}
		return err
	})
}

// CreateCampaign validates customer solvency and freezes the initial campaign budget within a single ACID transaction.
// The budget limit is subsequently synchronized to the sharded Redis cluster to enable low-latency pacing evaluation at the edge.
func (s *Service) CreateCampaign(ctx context.Context, customerID uuid.UUID, name string, budgetLimit decimal.Decimal, pacingMode db.PacingModeType, dailyBudget decimal.Decimal, timezone string, freqLimit, freqWindow int32, targetCountries []string, idempotencyKey string) (uuid.UUID, error) {
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
		if ads.FromNumeric(cust.Balance).LessThan(budgetLimit) {
			return fmt.Errorf("insufficient balance")
		}
		_, err = q.UpdateCustomerBalanceManagement(ctx, db.UpdateCustomerBalanceManagementParams{
			ID:      ads.ToUUID(customerID),
			Balance: ads.ToNumeric(budgetLimit.Neg()),
		})
		if err != nil {
			return err
		}
		_, err = q.CreateCampaign(ctx, db.CreateCampaignParams{
			ID:              ads.ToUUID(campaignID),
			Name:            name,
			BudgetLimit:     ads.ToNumeric(budgetLimit),
			Status:          db.CampaignStatusTypeACTIVE,
			CustomerID:      ads.ToUUID(customerID),
			PacingMode:      pacingMode,
			DailyBudget:     ads.ToNumeric(dailyBudget),
			Timezone:        timezone,
			FreqLimit:       pgtype.Int4{Int32: freqLimit, Valid: true},
			FreqWindow:      pgtype.Int4{Int32: freqWindow, Valid: true},
			TargetCountries: targetCountries,
		})
		if err != nil {
			return err
		}
		_, err = q.CreateLedgerEntry(ctx, db.CreateLedgerEntryParams{
			CustomerID:      ads.ToUUID(customerID),
			CampaignID:      ads.ToUUID(campaignID),
			Amount:          ads.ToNumeric(budgetLimit),
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
			payloadBytes, _ := json.Marshal(CampaignPayload{CampaignID: campaignID.String(), BudgetLimit: budgetLimit.StringFixed(2)})
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
		camp, err := q.GetCampaignFull(ctx, ads.ToUUID(campaignID))
		if err != nil {
			return err
		}
		if camp.Status != db.CampaignStatusTypeDRAINING {
			return nil
		}
		totalBudget := ads.FromNumeric(camp.BudgetLimit)
		currentSpend := ads.FromNumeric(camp.CurrentSpend)
		remaining := totalBudget.Sub(currentSpend)
		if remaining.IsNegative() {
			remaining = decimal.Zero
		}
		feePercent := decimal.NewFromFloat(s.cfg.Management.CancellationFeePercent).Div(decimal.NewFromInt(100))
		fee := remaining.Mul(feePercent).Round(2)
		refund := remaining.Sub(fee)
		if refund.IsPositive() {
			_, err = q.UpdateCustomerBalanceManagement(ctx, db.UpdateCustomerBalanceManagementParams{
				ID:      camp.CustomerID,
				Balance: ads.ToNumeric(refund),
			})
			if err != nil {
				return err
			}
			_, err = q.CreateLedgerEntry(ctx, db.CreateLedgerEntryParams{
				CustomerID: camp.CustomerID,
				CampaignID: ads.ToUUID(campaignID),
				Amount:     ads.ToNumeric(refund),
				Type:       db.LedgerTypeRELEASE,
			})
			if err != nil {
				return err
			}
		}
		if fee.IsPositive() {
			_, err = q.CreateLedgerEntry(ctx, db.CreateLedgerEntryParams{
				CustomerID: camp.CustomerID,
				CampaignID: ads.ToUUID(campaignID),
				Amount:     ads.ToNumeric(fee),
				Type:       db.LedgerTypeFEE,
			})
			if err != nil {
				return err
			}
			metrics.CommissionsCollectedTotal.Add(fee.InexactFloat64())
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
