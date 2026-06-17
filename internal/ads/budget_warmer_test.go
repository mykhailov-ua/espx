package ads

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"espx/internal/ads/db"
	"espx/internal/domain"
	"espx/internal/metrics"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/prometheus/client_golang/prometheus/testutil"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Redis mock that returns budget miss once then success for registry recovery tests.
type budgetMissOnceRedis struct {
	mockRedisClient
	calls atomic.Int32
}

func (m *budgetMissOnceRedis) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	if m.calls.Add(1) == 1 {
		cmd.SetVal(int64(-1))
		return cmd
	}
	cmd.SetVal(int64(0))
	return cmd
}

func (m *budgetMissOnceRedis) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	return m.EvalSha(ctx, "", keys, args...)
}

// Campaign repo that panics if PostgreSQL is touched during registry recovery.
type panicCampaignRepo struct{}

func (panicCampaignRepo) GetByID(context.Context, uuid.UUID) (*domain.Campaign, error) {
	panic("PG must not be called when registry has budget snapshot")
}

func (panicCampaignRepo) UpdateStatus(context.Context, uuid.UUID, domain.CampaignStatus) error {
	return nil
}

func (panicCampaignRepo) UpdateSpend(context.Context, uuid.UUID, int64, string) error {
	return nil
}

func (panicCampaignRepo) ListActive(context.Context) ([]*domain.Campaign, error) {
	return nil, nil
}

// Guards remaining budget micros never go negative and nil campaigns spend zero.
func TestRemainingBudgetMicro(t *testing.T) {
	assert.Equal(t, int64(0), RemainingBudgetMicro(nil))
	assert.Equal(t, int64(700), RemainingBudgetMicro(&domain.Campaign{BudgetLimit: 1000, CurrentSpend: 300}))
	assert.Equal(t, int64(0), RemainingBudgetMicro(&domain.Campaign{BudgetLimit: 100, CurrentSpend: 500}))
}

// Guards warm path never clobber existing Redis budget keys on cache hit.
func TestBudgetCacheWarmer_SetNXDoesNotOverwrite(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	campID := uuid.New()
	reg := &mockRegistry{}
	camp, ok := reg.GetCampaign(campID)
	require.True(t, ok)
	camp.BudgetLimit = 1_000_000
	camp.CurrentSpend = 200_000
	cachedMockCamp.Store(camp)

	require.NoError(t, rdb.Set(ctx, camp.BudgetCampaignKey, 42, 0).Err())

	w := NewBudgetCacheWarmer([]redis.UniversalClient{rdb}, NewJumpHashSharder(1))
	warmed, err := w.Warm(ctx, []*domain.Campaign{camp})
	require.NoError(t, err)
	assert.Equal(t, 0, warmed)

	val, err := rdb.Get(ctx, camp.BudgetCampaignKey).Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(42), val)
}

// Guards cold Redis shards receive remaining budget on first warm.
func TestBudgetCacheWarmer_insertsMissingKeys(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	campID := uuid.New()
	reg := &mockRegistry{}
	camp, ok := reg.GetCampaign(campID)
	require.True(t, ok)
	camp.BudgetLimit = 5_000_000
	camp.CurrentSpend = 1_000_000
	cachedMockCamp.Store(camp)

	w := NewBudgetCacheWarmer([]redis.UniversalClient{rdb}, NewJumpHashSharder(1))
	warmed, err := w.Warm(ctx, []*domain.Campaign{camp})
	require.NoError(t, err)
	assert.Equal(t, 1, warmed)

	val, err := rdb.Get(ctx, camp.BudgetCampaignKey).Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(4_000_000), val)
}

// Guards budget miss recovery uses in-memory registry before PostgreSQL.
func TestUnifiedFilter_budgetMiss_recoversFromRegistryWithoutPG(t *testing.T) {
	campID := uuid.New()
	custID := uuid.New()
	cachedMockCamp.Store(&domain.Campaign{
		ID:           campID,
		CustomerID:   custID,
		BudgetLimit:  10_000_000,
		CurrentSpend: 0,
	})
	t.Cleanup(func() { cachedMockCamp.Store(nil) })

	reg := &mockRegistry{}
	f := NewUnifiedFilter(
		[]redis.UniversalClient{&budgetMissOnceRedis{}},
		NewJumpHashSharder(1),
		reg,
		panicCampaignRepo{},
		1000,
		time.Minute,
		time.Hour,
		time.Hour,
		1_000_000,
		10_000,
		"events-budget-warm",
		10000,
	)

	beforePG := testutil.ToFloat64(metrics.BudgetCacheMissPGTotal)
	beforeRecover := testutil.ToFloat64(metrics.BudgetCacheRegistryRecoverTotal)

	err := f.Check(context.Background(), &domain.Event{
		CampaignID: campID,
		ClickID:    uuid.NewString(),
		Type:       "click",
		IP:         "1.1.1.1",
	})
	require.NoError(t, err)
	assert.Equal(t, beforePG, testutil.ToFloat64(metrics.BudgetCacheMissPGTotal))
	assert.Equal(t, beforeRecover+1, testutil.ToFloat64(metrics.BudgetCacheRegistryRecoverTotal))
}

// Guards incremental warm uses SET NX so existing keys are not overwritten.
func TestBudgetCacheWarmer_WarmOne_Incremental(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	campID := uuid.New()
	camp := &domain.Campaign{
		ID:                campID,
		BudgetLimit:       2_000_000,
		CurrentSpend:      500_000,
		BudgetCampaignKey: "budget:campaign:" + campID.String(),
	}

	w := NewBudgetCacheWarmer([]redis.UniversalClient{rdb}, NewJumpHashSharder(1))

	warmed, err := w.WarmOne(ctx, camp)
	require.NoError(t, err)
	assert.True(t, warmed)

	val, err := rdb.Get(ctx, camp.BudgetCampaignKey).Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(1_500_000), val)

	warmed2, err := w.WarmOne(ctx, camp)
	require.NoError(t, err)
	assert.False(t, warmed2)
}

// Guards single-campaign refresh updates registry and Redis without full sync.
func TestCampaignRegistry_UpdateAndWarmCampaign_Incremental(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	campID := uuid.New()
	custID := uuid.New()

	mock := &MockRepo{
		budgets: map[uuid.UUID]db.GetCampaignBudgetRow{
			campID: {
				ID:           pgtype.UUID{Bytes: campID, Valid: true},
				CustomerID:   pgtype.UUID{Bytes: custID, Valid: true},
				BudgetLimit:  3_000_000,
				CurrentSpend: 1_000_000,
				Status:       db.CampaignStatusTypeACTIVE,
			},
		},
	}

	r := newTestRegistry(t, mock)
	w := NewBudgetCacheWarmer([]redis.UniversalClient{rdb}, NewJumpHashSharder(1))
	r.SetBudgetWarmer(w)

	r.Add(campID, custID, nil, "", domain.PacingModeAsap, 1000, "UTC", 0, 0, nil)

	campBefore, ok := r.GetCampaign(campID)
	require.True(t, ok)
	assert.Equal(t, int64(1000), campBefore.DailyBudget)

	err := r.UpdateAndWarmCampaign(ctx, campID)
	require.NoError(t, err)

	campAfter, ok := r.GetCampaign(campID)
	require.True(t, ok)
	assert.Equal(t, int64(3_000_000), campAfter.BudgetLimit)
	assert.Equal(t, int64(1_000_000), campAfter.CurrentSpend)

	val, err := rdb.Get(ctx, campAfter.BudgetCampaignKey).Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(2_000_000), val)
}

// Guards pubsub campaign updates trigger incremental registry and Redis warm.
func TestCampaignRegistry_StartWatch_IncrementalWarm(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	campID := uuid.New()
	custID := uuid.New()

	mock := &MockRepo{
		budgets: map[uuid.UUID]db.GetCampaignBudgetRow{
			campID: {
				ID:           pgtype.UUID{Bytes: campID, Valid: true},
				CustomerID:   pgtype.UUID{Bytes: custID, Valid: true},
				BudgetLimit:  5_000_000,
				CurrentSpend: 1_000_000,
				Status:       db.CampaignStatusTypeACTIVE,
			},
		},
	}

	r := newTestRegistry(t, mock)
	w := NewBudgetCacheWarmer([]redis.UniversalClient{rdb}, NewJumpHashSharder(1))
	r.SetBudgetWarmer(w)

	r.Add(campID, custID, nil, "", domain.PacingModeAsap, 1000, "UTC", 0, 0, nil)

	channel := "test:campaign:updates:incremental"
	r.StartWatch(ctx, rdb, channel)

	time.Sleep(200 * time.Millisecond)

	err := rdb.Publish(ctx, channel, campID.String()).Err()
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		camp, ok := r.GetCampaign(campID)
		return ok && camp.BudgetLimit == 5_000_000 && camp.CurrentSpend == 1_000_000
	}, 2*time.Second, 50*time.Millisecond)

	val, err := rdb.Get(ctx, "budget:campaign:"+campID.String()).Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(4_000_000), val)
}

// No-op Redis client for budget warmer benchmark isolation.
type benchmarkRedisClient struct {
	redis.UniversalClient
}

// Pipeline stub that always succeeds SET NX for warmer benchmarks.
type benchmarkPipeliner struct {
	redis.Pipeliner
}

func (b *benchmarkPipeliner) SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd {
	cmd := redis.NewBoolCmd(ctx)
	cmd.SetVal(true)
	return cmd
}

func (b *benchmarkPipeliner) Exec(ctx context.Context) ([]redis.Cmder, error) {
	return nil, nil
}

func (r *benchmarkRedisClient) Pipeline() redis.Pipeliner {
	return &benchmarkPipeliner{}
}

func (r *benchmarkRedisClient) SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd {
	cmd := redis.NewBoolCmd(ctx)
	cmd.SetVal(true)
	return cmd
}

// Tracks WarmOne allocation and syscall cost for perf regression gates.
func BenchmarkBudgetCacheWarmer_WarmOne(b *testing.B) {
	ctx := context.Background()
	rdb := &benchmarkRedisClient{}
	w := NewBudgetCacheWarmer([]redis.UniversalClient{rdb}, NewJumpHashSharder(1))
	campID := uuid.New()
	camp := &domain.Campaign{
		ID:                campID,
		BudgetLimit:       1000,
		CurrentSpend:      100,
		BudgetCampaignKey: "budget:campaign:" + campID.String(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = w.WarmOne(ctx, camp)
	}
}

// Tracks batch warm cost when many campaigns refresh together.
func BenchmarkBudgetCacheWarmer_Warm(b *testing.B) {
	ctx := context.Background()
	rdb := &benchmarkRedisClient{}
	w := NewBudgetCacheWarmer([]redis.UniversalClient{rdb}, NewJumpHashSharder(1))
	campaigns := make([]*domain.Campaign, 10)
	for i := 0; i < 10; i++ {
		campID := uuid.New()
		campaigns[i] = &domain.Campaign{
			ID:                campID,
			BudgetLimit:       1000,
			CurrentSpend:      100,
			BudgetCampaignKey: "budget:campaign:" + campID.String(),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = w.Warm(ctx, campaigns)
	}
}
