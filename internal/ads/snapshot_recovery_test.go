package ads

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"espx/internal/domain"
	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

type MockPostgresDB struct {
	mu           sync.RWMutex
	spends       map[uuid.UUID]int64
	limits       map[uuid.UUID]int64
	idempotency  map[string]bool
	Healthy      atomic.Bool
	NetworkDelay atomic.Int64
}

func (m *MockPostgresDB) UpdateCampaignSpend(ctx context.Context, campaignID uuid.UUID, currentSpend int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.Healthy.Load() {
		return errors.New("postgres connection timeout")
	}
	m.spends[campaignID] = currentSpend
	return nil
}

func (m *MockPostgresDB) GetCampaignBudgetLimit(ctx context.Context, campaignID uuid.UUID) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.Healthy.Load() {
		return 0, errors.New("postgres connection timeout")
	}
	return m.limits[campaignID], nil
}

func (m *MockPostgresDB) GetCampaignSpend(ctx context.Context, campaignID uuid.UUID) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.Healthy.Load() {
		return 0, errors.New("postgres connection timeout")
	}
	return m.spends[campaignID], nil
}

func (m *MockPostgresDB) MarkEventIdempotent(ctx context.Context, clickID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.Healthy.Load() {
		return false, errors.New("postgres connection timeout")
	}
	if m.idempotency[clickID] {
		return false, nil
	}
	m.idempotency[clickID] = true
	return true, nil
}

type MockClickHouseDB struct {
	mu     sync.RWMutex
	events []*domain.Event
}

func (m *MockClickHouseDB) QueryEventsSince(ctx context.Context, since time.Time) ([]*domain.Event, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var res []*domain.Event
	for _, e := range m.events {
		if e.CreatedAt.After(since) {
			res = append(res, e)
		}
	}
	return res, nil
}

func (m *MockClickHouseDB) QueryAggregatedSpend(ctx context.Context, until time.Time) (map[uuid.UUID]int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make(map[uuid.UUID]int64)
	for _, e := range m.events {
		if !e.CreatedAt.After(until) {
			charge := int64(10_000)
			if e.Type == "impression" {
				charge = int64(1_000)
			}
			res[e.CampaignID] += charge
		}
	}
	return res, nil
}

func (m *MockClickHouseDB) LogEvent(e *domain.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()

	eCopy := &domain.Event{
		ClickID:    e.ClickID,
		CampaignID: e.CampaignID,
		Type:       e.Type,
		Payload:    e.Payload,
		IP:         e.IP,
		UA:         e.UA,
		CreatedAt:  e.CreatedAt,
	}
	m.events = append(m.events, eCopy)
}

func TestSnapshotRecovery_DisasterStressReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping heavy HA/DR PITR stress integration test")
	}

	campID := uuid.New()
	custID := uuid.New()
	reg := &mockRegistry{}

	staticCampaign.ID = campID
	staticCampaign.CustomerID = custID
	staticCampaign.IDStr = campID.String()
	staticCampaign.CustomerIDStr = custID.String()
	staticCampaign.IDStrAny = campID.String()
	staticCampaign.CustomerIDStrAny = custID.String()
	staticCampaign.DailyBudgetMicroAny = int64(50_000_000)
	staticCampaign.Location = time.UTC

	rdb, cleanup := setupTestRedis(t)
	defer cleanup()
	ctx := context.Background()

	pg := &MockPostgresDB{
		spends:      make(map[uuid.UUID]int64),
		limits:      map[uuid.UUID]int64{campID: int64(50_000_000)},
		idempotency: make(map[string]bool),
	}
	pg.Healthy.Store(true)

	ch := &MockClickHouseDB{}

	budgetSourceKey := "budget:campaign:" + campID.String()
	_ = rdb.Set(ctx, budgetSourceKey, int64(50_000_000), 24*time.Hour).Err()

	sharder := NewJumpHashSharder(1)
	f := NewUnifiedFilter(
		[]redis.UniversalClient{rdb},
		sharder,
		reg,
		nil,
		100000,
		time.Minute,
		time.Hour,
		time.Hour,
		10_000,
		1_000,
		"events-stream-sla",
		100000,
	)

	replicator := NewSnapshotReplicator(pg, ch, []redis.UniversalClient{rdb}, sharder, 10_000, 1_000)

	const concurrency = 20
	const iterations = 500
	var wg sync.WaitGroup
	wg.Add(concurrency)

	startTime := time.Now().Add(-5 * time.Minute)

	for g := 0; g < concurrency; g++ {
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				evtType := "click"
				if i%5 == 0 {
					evtType = "impression"
				}

				evt := &domain.Event{
					CampaignID: campID,
					ClickID:    fmt.Sprintf("clk_%d_%d", workerID, i),
					IP:         "192.168.1.100",
					Payload:    []byte(`{"bid_micro":5000}`),
					Type:       evtType,
					CreatedAt:  startTime.Add(time.Duration(workerID*10+i) * time.Second),
				}

				err := f.Check(ctx, evt)
				if err != nil {
					continue
				}

				ch.LogEvent(evt)
			}
		}(g)
	}

	wg.Wait()

	checkpointTime := startTime.Add(2 * time.Minute)
	snapshotData, err := replicator.CreateSnapshot(ctx, checkpointTime)
	assert.NoError(t, err)

	var snap Snapshot
	err = json.Unmarshal(snapshotData, &snap)
	assert.NoError(t, err)
	checkpointSpend := snap.CampaignSpends[campID]
	assert.Greater(t, checkpointSpend, int64(0))

	liveStart := checkpointTime.Add(time.Second)
	var postWg sync.WaitGroup
	postWg.Add(10)
	for g := 0; g < 10; g++ {
		go func(workerID int) {
			defer postWg.Done()
			for i := 0; i < 50; i++ {
				evt := &domain.Event{
					CampaignID: campID,
					ClickID:    fmt.Sprintf("post_clk_%d_%d", workerID, i),
					IP:         "192.168.1.100",
					Payload:    []byte(`{"bid_micro":5000}`),
					Type:       "click",
					CreatedAt:  liveStart.Add(time.Duration(workerID*10+i) * time.Second),
				}

				err := f.Check(ctx, evt)
				if err != nil {
					continue
				}

				ch.LogEvent(evt)
			}
		}(g)
	}
	postWg.Wait()

	totalActualSpend, err := ch.QueryAggregatedSpend(ctx, time.Now().Add(24*time.Hour))
	assert.NoError(t, err)
	expectedFinalSpend := totalActualSpend[campID]

	_ = rdb.Del(ctx, budgetSourceKey).Err()

	pg.spends[campID] = 0
	for k := range pg.idempotency {
		delete(pg.idempotency, k)
	}

	restoredSnap, err := replicator.RestoreSnapshot(ctx, snapshotData)
	assert.NoError(t, err)
	assert.Equal(t, checkpointSpend, restoredSnap.CampaignSpends[campID])

	assert.Equal(t, checkpointSpend, pg.spends[campID])

	redisBudget, err := rdb.Get(ctx, budgetSourceKey).Int64()
	assert.NoError(t, err)
	assert.Equal(t, 50_000_000-checkpointSpend, redisBudget)

	replayedCount, err := replicator.ReplayTelemetrySince(ctx, checkpointTime, f)
	assert.NoError(t, err)
	assert.Greater(t, replayedCount, 0)

	finalPgSpend := pg.spends[campID]

	assert.Equal(t, expectedFinalSpend, finalPgSpend)
}
