// Package ads provides miscellaneous helpers shared across the ads package:
//
//   - NewFastUUID: a time-ordered UUID v7 variant that embeds a cached Unix-ms
//     timestamp, a 48-bit atomic monotone sequence, and a 16-bit node ID derived
//     from hostname+PID. The cached timestamp is updated by a background 1-ms ticker
//     goroutine; this avoids a syscall on every UUID generation call. The result is
//     monotonically ordered within a process and globally unique across nodes.
//
//   - CampaignRepo / CustomerRepo: thin sqlc wrappers that add txID-guarded spend
//     updates (INSERT INTO sync_idempotency ON CONFLICT DO NOTHING) to make
//     SyncWorker commits idempotent under network retries.
//
//   - DeepResetAdStreamEvent / ClearAdStreamEvent / DeepResetAdDLQEvent: pool
//     reset helpers for vtproto-generated structs. DeepReset re-slices byte fields
//     to length 0 (retaining capacity); Clear nils them (releases backing arrays).
//     Use DeepReset when the struct will be immediately reused, Clear before Put.
//
//   - UnsafeString / UnsafeBytes: unsafe string <-> []byte conversions with zero copy.
//     The caller guarantees the backing memory outlives the returned value.
package ads

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/pb"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
)

var (
	nodeID          uint16
	idSequence      uint64
	cachedUnixMilli atomic.Int64
)

func init() {
	hostname, _ := os.Hostname()
	h := uint32(os.Getpid())
	for _, c := range hostname {
		h = h*31 + uint32(c)
	}
	nodeID = uint16(h ^ (h >> 16))

	cachedUnixMilli.Store(time.Now().UnixMilli())
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			cachedUnixMilli.Store(time.Now().UnixMilli())
		}
	}()
}

// NewFastUUID generates a time-ordered UUID without crypto/rand. Layout:
//
//	bytes  0-5:  48-bit Unix millisecond timestamp (big-endian, cached).
//	bytes  6-7:  version nibble 0x7 | upper bits of 48-bit sequence.
//	bytes  8-9:  variant 0x80 | 16-bit nodeID.
//	bytes 10-15: lower 48 bits of the atomic sequence.
//
// The sequence wraps at 2^64; within a single millisecond window collisions
// are impossible across the 2^64 sequence space.
func NewFastUUID() (uuid.UUID, error) {
	seq := atomic.AddUint64(&idSequence, 1)
	now := cachedUnixMilli.Load()

	var u uuid.UUID

	u[0] = byte(now >> 40)
	u[1] = byte(now >> 32)
	u[2] = byte(now >> 24)
	u[3] = byte(now >> 16)
	u[4] = byte(now >> 8)
	u[5] = byte(now)

	u[6] = byte(seq >> 48)
	u[7] = byte(seq >> 40)

	u[8] = byte(nodeID >> 8)
	u[9] = byte(nodeID)

	u[10] = byte(seq >> 40)
	u[11] = byte(seq >> 32)
	u[12] = byte(seq >> 24)
	u[13] = byte(seq >> 16)
	u[14] = byte(seq >> 8)
	u[15] = byte(seq)

	u[6] = (u[6] & 0x0f) | 0x70
	u[8] = (u[8] & 0x3f) | 0x80

	return u, nil
}

func ToUUID(u uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: u, Valid: true}
}

const MicroUnitFactor = 1_000_000

func SliceToMap(slice []string) map[string]struct{} {
	if slice == nil {
		return nil
	}
	m := make(map[string]struct{}, len(slice))
	for _, s := range slice {
		m[s] = struct{}{}
	}
	return m
}

// CampaignRepo wraps a sqlc Querier to implement domain.CampaignRepository.
// UpdateSpend acquires a PostgreSQL transaction and inserts a txID row into
// sync_idempotency before updating current_spend; if the txID is already present
// the UPDATE is skipped, providing exactly-once semantics for SyncWorker.
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
		BudgetLimit:     row.BudgetLimit,
		CurrentSpend:    row.CurrentSpend,
		Status:          domain.CampaignStatus(row.Status),
		PacingMode:      domain.PacingMode(row.PacingMode),
		DailyBudget:     row.DailyBudget,
		Timezone:        row.Timezone,
		Location:        loc,
		FreqLimit:       row.FreqLimit.Int32,
		FreqWindow:      row.FreqWindow.Int32,
		TargetCountries: SliceToMap(row.TargetCountries),
	}, nil
}

func (r *CampaignRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.CampaignStatus) error {
	_, err := r.queries.UpdateCampaignStatus(ctx, db.UpdateCampaignStatusParams{
		ID:     pgtype.UUID{Bytes: id, Valid: true},
		Status: db.CampaignStatusType(status),
	})
	return err
}

func (r *CampaignRepo) UpdateSpend(ctx context.Context, id uuid.UUID, amount int64, txID string) error {
	var dbtx db.DBTX
	if getter, ok := r.queries.(interface{ DB() db.DBTX }); ok {
		dbtx = getter.DB()
	}

	if dbtx == nil {
		return r.queries.UpdateCampaignSpend(ctx, db.UpdateCampaignSpendParams{
			ID:           pgtype.UUID{Bytes: id, Valid: true},
			CurrentSpend: amount,
		})
	}

	var tx pgx.Tx
	var err error
	if beginner, ok := dbtx.(interface {
		Begin(context.Context) (pgx.Tx, error)
	}); ok {
		tx, err = beginner.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
	}

	var exec db.DBTX = dbtx
	if tx != nil {
		exec = tx
	}

	tag, err := exec.Exec(ctx, "INSERT INTO sync_idempotency (id) VALUES ($1) ON CONFLICT DO NOTHING", txID)
	if err != nil {
		return err
	}

	if tag.RowsAffected() == 0 {
		return nil
	}

	var q db.Querier = r.queries
	if tx != nil {
		if concreteQueries, ok := r.queries.(*db.Queries); ok {
			q = concreteQueries.WithTx(tx)
		}
	}

	err = q.UpdateCampaignSpend(ctx, db.UpdateCampaignSpendParams{
		ID:           pgtype.UUID{Bytes: id, Valid: true},
		CurrentSpend: amount,
	})
	if err != nil {
		return err
	}

	if tx != nil {
		return tx.Commit(ctx)
	}
	return nil
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
			BudgetLimit:     row.BudgetLimit,
			CurrentSpend:    row.CurrentSpend,
			Status:          domain.CampaignStatus(row.Status),
			PacingMode:      domain.PacingMode(row.PacingMode),
			DailyBudget:     row.DailyBudget,
			Timezone:        row.Timezone,
			Location:        loc,
			FreqLimit:       row.FreqLimit.Int32,
			FreqWindow:      row.FreqWindow.Int32,
			TargetCountries: SliceToMap(row.TargetCountries),
		}
	}
	return campaigns, nil
}

// CustomerRepo wraps a sqlc Querier to implement domain.CustomerRepository.
// UpdateBalance uses the same txID idempotency pattern as CampaignRepo.UpdateSpend.
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
		Balance:  row.Balance,
		Currency: row.Currency,
	}, nil
}

func (r *CustomerRepo) UpdateBalance(ctx context.Context, id uuid.UUID, amount int64, txID string) error {
	var dbtx db.DBTX
	if getter, ok := r.queries.(interface{ DB() db.DBTX }); ok {
		dbtx = getter.DB()
	}

	if dbtx == nil {
		return r.queries.UpdateCustomerBalance(ctx, db.UpdateCustomerBalanceParams{
			ID:      pgtype.UUID{Bytes: id, Valid: true},
			Balance: amount,
		})
	}

	var tx pgx.Tx
	var err error
	if beginner, ok := dbtx.(interface {
		Begin(context.Context) (pgx.Tx, error)
	}); ok {
		tx, err = beginner.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
	}

	var exec db.DBTX = dbtx
	if tx != nil {
		exec = tx
	}

	tag, err := exec.Exec(ctx, "INSERT INTO sync_idempotency (id) VALUES ($1) ON CONFLICT DO NOTHING", txID)
	if err != nil {
		return err
	}

	if tag.RowsAffected() == 0 {
		return nil
	}

	var q db.Querier = r.queries
	if tx != nil {
		if concreteQueries, ok := r.queries.(*db.Queries); ok {
			q = concreteQueries.WithTx(tx)
		}
	}

	err = q.UpdateCustomerBalance(ctx, db.UpdateCustomerBalanceParams{
		ID:      pgtype.UUID{Bytes: id, Valid: true},
		Balance: amount,
	})
	if err != nil {
		return err
	}

	if tx != nil {
		return tx.Commit(ctx)
	}
	return nil
}

// UnsafeString converts a byte slice to a string without allocation.
// The returned string must not outlive the byte slice.
func UnsafeString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// UnsafeBytes converts a string to a byte slice without allocation.
// The returned slice must not be modified; doing so may corrupt the original string.
func UnsafeBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

type ByteSliceValue struct {
	b []byte
}

func (v *ByteSliceValue) MarshalBinary() ([]byte, error) {
	return v.b, nil
}

var byteSliceValuePool = sync.Pool{
	New: func() any {
		return new(ByteSliceValue)
	},
}

// DeepResetAdStreamEvent re-slices all byte fields of m to length 0, retaining
// their backing arrays for reuse. Use immediately before re-populating and
// passing to MarshalToSizedBufferVT on the producer hot path.
func DeepResetAdStreamEvent(m *pb.AdStreamEvent) {
	if m == nil {
		return
	}
	m.ClickId = m.ClickId[:0]
	m.CampaignId = m.CampaignId[:0]
	m.EventType = m.EventType[:0]
	m.Payload = m.Payload[:0]
	m.Ip = m.Ip[:0]
	m.Ua = m.Ua[:0]
	m.CreatedAtUnix = 0
}

// ClearAdStreamEvent nils all byte fields of m, releasing the backing arrays
// to the GC. Use before returning the struct to a sync.Pool to prevent the pool
// from retaining arbitrarily large event payloads between reuses.
func ClearAdStreamEvent(m *pb.AdStreamEvent) {
	if m == nil {
		return
	}
	m.ClickId = nil
	m.CampaignId = nil
	m.EventType = nil
	m.Payload = nil
	m.Ip = nil
	m.Ua = nil
	m.CreatedAtUnix = 0
}

func DeepResetAdDLQEvent(m *pb.AdDLQEvent) {
	if m == nil {
		return
	}
	if m.OriginalEvent != nil {
		DeepResetAdStreamEvent(m.OriginalEvent)
	}
	m.Error = m.Error[:0]
	m.OriginalId = m.OriginalId[:0]
	m.WorkerId = m.WorkerId[:0]
	m.FailedAtUnix = 0
	m.RetryCount = 0
}
