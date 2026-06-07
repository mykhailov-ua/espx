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

type MockDBHealthWithDelay struct {
	Healthy atomic.Bool
	Delay   atomic.Int64
}

func (m *MockDBHealthWithDelay) Ping(ctx context.Context) error {
	d := m.Delay.Load()
	if d > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(d) * time.Millisecond):
		}
	}
	if !m.Healthy.Load() {
		return errors.New("simulated pgx pool offline")
	}
	return nil
}

func TestUnifiedFilter_LatencySLA(t *testing.T) {
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	f.SetSLATargets(
		200.0,
		100.0,
		100*time.Millisecond,
		0.5,
	)
	f.ResizeTrackers(10)

	mockDB := &MockDBHealthWithDelay{}
	mockDB.Healthy.Store(true)
	mockDB.Delay.Store(0)
	f.SetDBHealthChecker(mockDB)

	f.StartSLASentinel(ctx, 10*time.Millisecond)

	time.Sleep(50 * time.Millisecond)
	assert.False(t, f.slaPenaltyActive.Load(), "SLA penalty should be inactive initially")

	evt1 := &domain.Event{
		CampaignID: campID,
		ClickID:    uuid.NewString(),
		IP:         "1.1.1.1",
		Payload:    []byte(`{"bid_micro":1000000}`),
		Type:       "click",
	}

	beforeBudget1, _ := rdb.Get(ctx, budgetSourceKey).Int64()
	err := f.Check(ctx, evt1)
	assert.NoError(t, err)
	afterBudget1, _ := rdb.Get(ctx, budgetSourceKey).Int64()
	assert.Equal(t, int64(1_000_000), beforeBudget1-afterBudget1, "Should charge full click amount under normal SLA state")

	mockDB.Delay.Store(300)

	time.Sleep(500 * time.Millisecond)
	assert.True(t, f.slaPenaltyActive.Load(), "SLA penalty should auto-activate on slow DB latency")

	redisVal, err := rdb.Get(ctx, "sla:penalty:active").Bool()
	assert.NoError(t, err)
	assert.True(t, redisVal, "Redis key should be active")

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
	assert.Equal(t, int64(500_000), beforeBudget2-afterBudget2, "Should apply 50% discount charge while SLA penalty is active")

	mockDB.Delay.Store(0)

	time.Sleep(400 * time.Millisecond)
	assert.False(t, f.slaPenaltyActive.Load(), "SLA penalty should deactivate automatically once latency stabilizes")

	_, err = rdb.Get(ctx, "sla:penalty:active").Bool()
	assert.ErrorIs(t, err, redis.Nil, "Redis key should be cleared after recovery")

	evt3 := &domain.Event{
		CampaignID: campID,
		ClickID:    uuid.NewString(),
		IP:         "1.1.1.1",
		Payload:    []byte(`{"bid_micro":1000000}`),
		Type:       "click",
	}

	beforeBudget3, _ := rdb.Get(ctx, budgetSourceKey).Int64()
	err = f.Check(ctx, evt3)
	assert.NoError(t, err)
	afterBudget3, _ := rdb.Get(ctx, budgetSourceKey).Int64()
	assert.Equal(t, int64(1_000_000), beforeBudget3-afterBudget3, "Should charge full amount again after recovery")
}
