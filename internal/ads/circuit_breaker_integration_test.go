package ads

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FailingEventStore returns an error for every StoreBatch call until
// the heal channel is closed. It also counts the number of calls.
type FailingEventStore struct {
	mu      sync.Mutex
	flushes [][]*domain.Event
	calls   atomic.Int64
	failErr error
	healed  atomic.Bool
}

func (m *FailingEventStore) StoreBatch(ctx context.Context, events []*domain.Event) error {
	m.calls.Add(1)
	if !m.healed.Load() {
		return m.failErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	batchCopy := make([]*domain.Event, len(events))
	copy(batchCopy, events)
	m.flushes = append(m.flushes, batchCopy)
	return nil
}

func (m *FailingEventStore) Close() error { return nil }

func (m *FailingEventStore) Heal() { m.healed.Store(true) }

func TestStreamConsumer_CircuitBreakerStopsReads(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	failStore := &FailingEventStore{
		failErr: errors.New("database connection refused"),
	}

	// maxRetries=3, retryMaxWait=50ms -> CB openTimeout = 100ms
	producer := NewStreamProducer(rdb, "cb-test", 1000, 1*time.Second)
	consumer := NewStreamConsumer(
		failStore, rdb, "cb-test", "cb-group", "cb-c",
		2, 1,
		50*time.Millisecond, // flushInt
		1*time.Second,       // writeTimeout
		10*time.Millisecond, // retryInitWait
		50*time.Millisecond, // retryMaxWait
		3,                   // maxRetries
		1*time.Minute,       // streamMinIdle
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Produce events that will trigger flushes.
	for i := 0; i < 5; i++ {
		err := producer.Process(&domain.Event{CampaignID: uuid.New(), Type: "click"})
		require.NoError(t, err)
	}

	consumer.Start(ctx)

	// Wait for the CB to trip open. With 3 failures at 10-50ms backoff, it
	// should take ~200ms. Give it generous time.
	assert.Eventually(t, func() bool {
		return consumer.cb.State() == CircuitOpen
	}, 3*time.Second, 10*time.Millisecond, "circuit breaker should be open")

	// Record the call count when the CB is open.
	callsAtOpen := failStore.calls.Load()

	// Wait 200ms and verify calls have NOT increased significantly.
	// In a tight loop without CB, we'd see hundreds of calls.
	time.Sleep(200 * time.Millisecond)
	callsAfterWait := failStore.calls.Load()

	// Allow at most 2 extra calls (the HalfOpen probe).
	assert.LessOrEqual(t, callsAfterWait-callsAtOpen, int64(2),
		"CB should prevent flush calls while open, got %d extra calls", callsAfterWait-callsAtOpen)

	// Heal the store and verify recovery.
	failStore.Heal()

	// The CB will transition to HalfOpen after its timeout, then probe once.
	assert.Eventually(t, func() bool {
		return consumer.cb.State() == CircuitClosed
	}, 3*time.Second, 10*time.Millisecond, "circuit breaker should recover to closed")

	consumer.Close()
	consumer.Wait()
}
