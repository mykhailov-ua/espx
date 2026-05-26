package ads

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

type MockDBHealth struct {
	Healthy atomic.Bool
}

func (m *MockDBHealth) Ping(ctx context.Context) error {
	if !m.Healthy.Load() {
		return errors.New("simulated pgx pool offline")
	}
	return nil
}

func TestUnifiedFilter_SLAPenalty_Discount(t *testing.T) {
	campID := uuid.New()
	custID := uuid.New()
	reg := &mockRegistry{}

	// Setup staticCampaign for the mock registry
	staticCampaign.ID = campID
	staticCampaign.CustomerID = custID
	staticCampaign.IDStr = campID.String()
	staticCampaign.CustomerIDStr = custID.String()
	staticCampaign.IDStrAny = campID.String()
	staticCampaign.CustomerIDStrAny = custID.String()
	staticCampaign.DailyBudgetMicroAny = int64(10_000_000)
	staticCampaign.Location = time.UTC

	rdb, cleanup := setupTestRedis(t)
	defer cleanup()
	ctx := context.Background()

	// Clear any SLA penalty flags
	_ = rdb.Del(ctx, "sla:penalty:active").Err()

	// Pre-seed campaign budget in Redis
	budgetSourceKey := "budget:campaign:" + campID.String()
	_ = rdb.Set(ctx, budgetSourceKey, int64(10_000_000), 24*time.Hour).Err()

	f := NewUnifiedFilter(
		[]redis.UniversalClient{rdb},
		NewJumpHashSharder(1),
		reg,
		nil,
		1000,
		time.Minute,
		time.Hour,
		time.Hour,
		1_000_000, // Click charge: 1.00 USD
		10_000,    // Impression charge
		"events-stream-sla",
		10000,
	)

	// Case 1: SLA Penalty inactive. Charge should be exactly clickAmount (1_000_000).
	evt := &domain.Event{
		CampaignID: campID,
		ClickID:    uuid.NewString(),
		IP:         "1.1.1.1",
		Payload:    []byte(`{"bid_micro":1000000}`),
		Type:       "click",
	}

	beforeBudget, _ := rdb.Get(ctx, budgetSourceKey).Int64()
	err := f.Check(ctx, evt)
	assert.NoError(t, err)

	afterBudget, _ := rdb.Get(ctx, budgetSourceKey).Int64()
	assert.Equal(t, int64(1_000_000), beforeBudget-afterBudget)

	// Case 2: Manually activate SLA penalty.
	_ = rdb.Set(ctx, "sla:penalty:active", "true", time.Minute).Err()
	// Fetch state into local cache of filter
	f.slaPenaltyActive.Store(true)

	evt2 := &domain.Event{
		CampaignID: campID,
		ClickID:    uuid.NewString(),
		IP:         "1.1.1.1",
		Payload:    []byte(`{"bid_micro":1000000}`),
		Type:       "click",
	}

	beforeBudget2, _ := rdb.Get(ctx, budgetSourceKey).Int64()
	err = f.Check(ctx, evt2)
	assert.NoError(t, err)

	afterBudget2, _ := rdb.Get(ctx, budgetSourceKey).Int64()
	// Charge should be discounted by 50% -> exactly 500_000 micro-units
	assert.Equal(t, int64(500_000), beforeBudget2-afterBudget2)

	// Reset SLA penalty key
	_ = rdb.Del(ctx, "sla:penalty:active").Err()
}

func TestUnifiedFilter_SLASentinel_AutoDetection(t *testing.T) {
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = rdb.Del(ctx, "sla:penalty:active").Err()

	f := NewUnifiedFilter(
		[]redis.UniversalClient{rdb},
		NewJumpHashSharder(1),
		&mockRegistry{},
		nil,
		10,
		time.Minute,
		time.Hour,
		time.Hour,
		1000,
		100,
		"events-stream-sla",
		100,
	)

	f.SetSLATargets(
		200.0,
		100.0,
		10*time.Millisecond, // fast recovery duration
		0.9,                 // high alpha for fast decay
	)
	f.ResizeTrackers(2)

	mockDB := &MockDBHealth{}
	mockDB.Healthy.Store(true)
	f.SetDBHealthChecker(mockDB)

	// Start SLA Sentinel
	f.StartSLASentinel(ctx, 10*time.Millisecond)

	// Wait for first tick
	time.Sleep(30 * time.Millisecond)
	assert.False(t, f.slaPenaltyActive.Load())

	// Break the database connection
	mockDB.Healthy.Store(false)

	// Wait for database failure detection tick
	time.Sleep(50 * time.Millisecond)
	assert.True(t, f.slaPenaltyActive.Load())

	// Verify Redis flag has been auto-set by sentinel
	redisVal, err := rdb.Get(ctx, "sla:penalty:active").Bool()
	assert.NoError(t, err)
	assert.True(t, redisVal)

	// Heal the database
	mockDB.Healthy.Store(true)

	// Wait for recovery tick (EMA decay and stabilization)
	time.Sleep(150 * time.Millisecond)
	assert.False(t, f.slaPenaltyActive.Load())

	// Redis flag must be cleared/deleted or false
	redisVal, err = rdb.Get(ctx, "sla:penalty:active").Bool()
	assert.ErrorIs(t, err, redis.Nil)
}

func TestSLASentinel_NoRedisPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := NewUnifiedFilter(
		[]redis.UniversalClient{},
		NewJumpHashSharder(1),
		&mockRegistry{},
		nil,
		10,
		time.Minute,
		time.Hour,
		time.Hour,
		1000,
		100,
		"events-stream-sla",
		100,
	)

	f.SetSLATargets(
		200.0,
		100.0,
		10*time.Millisecond, // fast recovery duration
		0.9,                 // high alpha for fast decay
	)
	f.ResizeTrackers(2)

	mockDB := &MockDBHealth{}
	mockDB.Healthy.Store(false) // immediately broken (Ping-error simulation)
	f.SetDBHealthChecker(mockDB)

	// Start SLA Sentinel.
	// Since rdbs slice is empty, any redis calls must be bypassed entirely.
	// We verify that the process transitions to SLA penalty active in-memory and does not panic.
	assert.NotPanics(t, func() {
		f.StartSLASentinel(ctx, 10*time.Millisecond)
		time.Sleep(50 * time.Millisecond)
	})

	assert.True(t, f.slaPenaltyActive.Load(), "In-memory SLA penalty must be activated due to DB Ping-error")
}
