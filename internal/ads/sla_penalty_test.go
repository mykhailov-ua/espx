package ads

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"espx/internal/domain"
	"github.com/google/uuid"
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

	_ = rdb.Del(ctx, "sla:penalty:active").Err()

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
		1_000_000,
		10_000,
		"events-stream-sla",
		10000,
	)

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

	_ = rdb.Set(ctx, "sla:penalty:active", "true", time.Minute).Err()

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

	assert.Equal(t, int64(500_000), beforeBudget2-afterBudget2)

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
		10*time.Millisecond,
		0.9,
	)
	f.ResizeTrackers(2)

	mockDB := &MockDBHealth{}
	mockDB.Healthy.Store(true)
	f.SetDBHealthChecker(mockDB)

	f.StartSLASentinel(ctx, 10*time.Millisecond)

	time.Sleep(30 * time.Millisecond)
	assert.False(t, f.slaPenaltyActive.Load())

	mockDB.Healthy.Store(false)

	time.Sleep(50 * time.Millisecond)
	assert.True(t, f.slaPenaltyActive.Load())

	redisVal, err := rdb.Get(ctx, "sla:penalty:active").Bool()
	assert.NoError(t, err)
	assert.True(t, redisVal)

	mockDB.Healthy.Store(true)

	time.Sleep(150 * time.Millisecond)
	assert.False(t, f.slaPenaltyActive.Load())

	_, err = rdb.Get(ctx, "sla:penalty:active").Bool()
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
		10*time.Millisecond,
		0.9,
	)
	f.ResizeTrackers(2)

	mockDB := &MockDBHealth{}
	mockDB.Healthy.Store(false)
	f.SetDBHealthChecker(mockDB)

	assert.NotPanics(t, func() {
		f.StartSLASentinel(ctx, 10*time.Millisecond)
		time.Sleep(50 * time.Millisecond)
	})

	assert.True(t, f.slaPenaltyActive.Load(), "In-memory SLA penalty must be activated due to DB Ping-error")
}
