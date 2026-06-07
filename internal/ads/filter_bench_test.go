package ads

import (
	"context"
	"testing"
	"time"

	"espx/internal/domain"
	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

var (
	staticCmd       = redis.NewCmd(context.Background())
	staticStatusCmd = redis.NewStatusCmd(context.Background())
	staticStringCmd = redis.NewStringCmd(context.Background())
	staticBoolCmd   = redis.NewBoolCmd(context.Background())
)

type mockRedisClient struct {
	redis.UniversalClient
}

func (m *mockRedisClient) Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd {
	return staticStatusCmd
}

func (m *mockRedisClient) Get(ctx context.Context, key string) *redis.StringCmd {
	staticStringCmd.SetVal("1716223400000")
	return staticStringCmd
}

func (m *mockRedisClient) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	staticCmd.SetVal(int64(0))
	return staticCmd
}

func (m *mockRedisClient) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	staticCmd.SetVal(int64(0))
	return staticCmd
}

func (m *mockRedisClient) ScriptLoad(ctx context.Context, script string) *redis.StringCmd {
	staticStringCmd.SetVal("d3b07384d113edec49eaa6238ad5ff00")
	return staticStringCmd
}

func (m *mockRedisClient) SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd {
	staticBoolCmd.SetVal(true)
	return staticBoolCmd
}

func BenchmarkGeoFilter(b *testing.B) {
	geo := &MockGeoProvider{}
	registry := &mockRegistry{}
	f := NewGeoFilter(geo, registry)
	evt := &domain.Event{
		IP:         "1.1.1.1",
		CampaignID: uuid.New(),
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f.Check(ctx, evt)
	}
}

func BenchmarkFraudFilter_DC(b *testing.B) {
	geo := &MockGeoProvider{}
	f := NewFraudFilter(geo, nil, 300*time.Millisecond)
	evt := &domain.Event{
		IP: "1.1.1.66",
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f.Check(ctx, evt)
	}
}

func BenchmarkFraudFilter_CheckImpression(b *testing.B) {
	geo := &MockGeoProvider{}
	rdb := &mockRedisClient{}
	f := NewFraudFilter(geo, rdb, 300*time.Millisecond)
	evt := &domain.Event{
		Type:       "impression",
		IP:         "1.1.1.1",
		UserID:     "user123",
		CampaignID: uuid.New(),
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f.Check(ctx, evt)
	}
}

func BenchmarkFraudFilter_CheckClick(b *testing.B) {
	geo := &MockGeoProvider{}
	rdb := &mockRedisClient{}
	f := NewFraudFilter(geo, rdb, 300*time.Millisecond)
	evt := &domain.Event{
		Type:       "click",
		IP:         "1.1.1.1",
		UserID:     "user123",
		CampaignID: uuid.New(),
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f.Check(ctx, evt)
	}
}

func BenchmarkIPRateLimiter_Check(b *testing.B) {
	rdb := &mockRedisClient{}
	l := NewIPRateLimiter(rdb, 100, 10*time.Minute)
	evt := &domain.Event{
		IP: "192.168.1.1",
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = l.Check(ctx, evt)
	}
}

func BenchmarkDuplicateEventFilter_Check(b *testing.B) {
	rdb := &mockRedisClient{}
	f := NewDuplicateEventFilter(rdb, 1*time.Hour)
	evt := &domain.Event{
		Type:    "click",
		ClickID: "click123",
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f.Check(ctx, evt)
	}
}

func BenchmarkKeyFormatting_FraudFilter(b *testing.B) {
	evt := &domain.Event{
		UserID:     "user123",
		CampaignID: uuid.New(),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := bufPool.Get().(*bufWrapper)
		w.buf = w.buf[:0]
		w.buf = append(w.buf, "imp_ts:"...)
		w.buf = append(w.buf, evt.UserID...)
		w.buf = append(w.buf, ':')
		w.buf = appendUUID(w.buf, evt.CampaignID)
		key := unsafeString(w.buf)
		_ = key
		bufPool.Put(w)
	}
}

func BenchmarkKeyFormatting_IPRateLimiter(b *testing.B) {
	evt := &domain.Event{
		IP: "192.168.1.1",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := bufPool.Get().(*bufWrapper)
		w.buf = w.buf[:0]
		w.buf = append(w.buf, "ratelimit:ip:"...)
		w.buf = append(w.buf, evt.IP...)
		key := unsafeString(w.buf)
		_ = key
		bufPool.Put(w)
	}
}

func BenchmarkKeyFormatting_DuplicateEventFilter(b *testing.B) {
	evt := &domain.Event{
		Type:    "click",
		ClickID: "click123",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := bufPool.Get().(*bufWrapper)
		w.buf = w.buf[:0]
		w.buf = append(w.buf, "dup:"...)
		w.buf = append(w.buf, evt.Type...)
		w.buf = append(w.buf, ':')
		w.buf = append(w.buf, evt.ClickID...)
		key := unsafeString(w.buf)
		_ = key
		bufPool.Put(w)
	}
}

func BenchmarkUnifiedFilter_Check(b *testing.B) {
	rdb := &mockRedisClient{}
	sharder := NewJumpHashSharder(1)
	registry := &mockRegistry{}

	f := NewUnifiedFilter(
		[]redis.UniversalClient{rdb},
		sharder,
		registry,
		nil,
		100,
		time.Minute,
		time.Hour,
		time.Hour,
		100_000,
		10_000,
		"events",
		10000,
	)

	evt := &domain.Event{
		Type:       "click",
		IP:         "1.1.1.1",
		UserID:     "user123",
		CampaignID: uuid.New(),
		ClickID:    "click123",
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f.Check(ctx, evt)
	}
}

func BenchmarkRedisBudgetManager_CheckAndSpend(b *testing.B) {
	rdb := &mockRedisClient{}
	bm := NewRedisBudgetManager(rdb, nil, time.Hour)

	ctx := context.Background()
	customerID := uuid.New()
	campaignID := uuid.New()
	clickID := "click123"
	amount := int64(100_000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bm.CheckAndSpend(ctx, customerID, campaignID, clickID, amount)
	}
}
