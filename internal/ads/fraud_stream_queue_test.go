package ads

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"espx/internal/domain"
	"espx/internal/metrics"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Redis stub counting XAdd calls for fraud queue flush tests.
type countingRedisXAdd struct {
	mockRedisClient
	xadds atomic.Int32
}

func (m *countingRedisXAdd) Pipeline() redis.Pipeliner {
	parent := m
	return &countingPipeliner{
		mockPipeliner: mockPipeliner{
			incrCmd: redis.NewIntCmd(context.Background()),
			doCmd:   redis.NewCmd(context.Background()),
		},
		parent: parent,
	}
}

// Test helper type for countingPipeliner scenarios.
type countingPipeliner struct {
	mockPipeliner
	parent *countingRedisXAdd
}

func (p *countingPipeliner) XAdd(ctx context.Context, args *redis.XAddArgs) *redis.StringCmd {
	p.parent.xadds.Add(1)
	return p.mockPipeliner.XAdd(ctx, args)
}

// Guards fraud events enqueue and flush to Redis stream without loss.
func TestFraudStreamWriter_enqueueAndFlush(t *testing.T) {
	rdb := &countingRedisXAdd{}
	q := NewFraudStreamWriter([]redis.UniversalClient{rdb}, "fraud-stream", 1000)
	require.NotNil(t, q)
	defer q.Stop()

	evt := &domain.Event{
		ClickID:     "click-1",
		CampaignID:  uuid.New(),
		UserID:      "user-1",
		Type:        "click",
		IP:          "1.1.1.1",
		UA:          "test-agent",
		Payload:     []byte(`{"k":"v"}`),
		FraudReason: "ttc",
	}
	require.True(t, q.Enqueue(0, evt))

	require.Eventually(t, func() bool {
		return rdb.xadds.Load() == 1 && q.Pending() == 0
	}, time.Second, 2*time.Millisecond)
}

// Guards full fraud ring buffer increments drop metric instead of blocking ingest.
func TestFraudStreamWriter_ringFullIncrementsDropMetric(t *testing.T) {
	rdb := &mockRedisClient{}
	q := &FraudStreamWriter{
		stream: "fraud-stream",
		maxLen: 1000,
		rdbs:   []redis.UniversalClient{rdb},
		stopCh: make(chan struct{}),
	}
	q.allocCursor = fraudRingUsable
	q.writeCursor = fraudRingUsable

	before := testutil.ToFloat64(metrics.FraudStreamDropTotal)
	evt := &domain.Event{ClickID: "c1", CampaignID: uuid.New(), Type: "click"}
	enqueueFraudReject(q, 0, evt)
	assert.Equal(t, before+1, testutil.ToFloat64(metrics.FraudStreamDropTotal))
}

// Guards concurrent fraud enqueue does not corrupt ring buffer state.
func TestFraudStreamWriter_concurrentEnqueue(t *testing.T) {
	rdb := &countingRedisXAdd{}
	q := NewFraudStreamWriter([]redis.UniversalClient{rdb}, "fraud-stream", 1000)
	require.NotNil(t, q)
	defer q.Stop()

	const producers = 8
	const perProducer = 128
	var wg sync.WaitGroup
	wg.Add(producers)
	for p := 0; p < producers; p++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				evt := &domain.Event{
					ClickID:    "click",
					CampaignID: uuid.New(),
					Type:       "click",
				}
				q.Enqueue(0, evt)
			}
		}()
	}
	wg.Wait()
	q.Stop()

	assert.Greater(t, rdb.xadds.Load(), int32(0))
}

// Tracks fraud stream enqueue cost on the rejection hot path.
func BenchmarkFraudStreamWriter_Enqueue(b *testing.B) {
	q := NewFraudStreamWriter(nil, "fraud-stream", 1000)
	if q == nil {
		q = &FraudStreamWriter{stream: "fraud-stream", stopCh: make(chan struct{})}
	}
	evt := &domain.Event{
		ClickID:    "click-1",
		CampaignID: uuid.New(),
		UserID:     "user-1",
		Type:       "click",
		IP:         "1.1.1.1",
		Payload:    []byte(`{"k":"v"}`),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Enqueue(0, evt)
	}
}
