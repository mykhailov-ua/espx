package ads

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"espx/internal/domain"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
)

func setupTestRedis(t *testing.T) (redis.UniversalClient, func()) {
	ctx := context.Background()
	redisContainer, err := rediscontainer.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("failed to start redis container: %s", err)
	}
	endpoint, err := redisContainer.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("failed to get redis endpoint: %s", err)
	}
	rdb := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{endpoint},
	})
	return rdb, func() {
		_ = rdb.Close()
		_ = redisContainer.Terminate(ctx)
	}
}

type MockEventStore struct {
	mu      sync.Mutex
	flushes [][]*domain.Event
	Err     error
}

func (m *MockEventStore) StoreBatch(ctx context.Context, events []*domain.Event) error {
	if m.Err != nil {
		return m.Err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	batchCopy := make([]*domain.Event, len(events))
	copy(batchCopy, events)
	m.flushes = append(m.flushes, batchCopy)
	return nil
}

func (m *MockEventStore) Close() error {
	return nil
}

func TestStreamConsumer_Ingestion(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	producer := NewStreamProducer(rdb, "s1", 1000, 1*time.Second)

	err := producer.Process(&domain.Event{CampaignID: uuid.New(), Type: "click"})
	assert.NoError(t, err)
}

func TestStreamConsumer_BatchFlushing(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	mockStore := &MockEventStore{}
	producer := NewStreamProducer(rdb, "s2", 1000, 1*time.Second)
	proc := NewStreamConsumer(mockStore, rdb, "s2", "g2", "c2", 2, 1, 10*time.Second, 1*time.Second, 10*time.Millisecond, 100*time.Millisecond, 3, 1*time.Minute, 1*time.Second)

	proc.Start(context.Background())
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 3; i++ {
		_ = producer.Process(&domain.Event{CampaignID: uuid.New(), Type: "click"})
	}

	time.Sleep(200 * time.Millisecond)
	proc.Close()
	proc.Wait(context.Background())

	mockStore.mu.Lock()
	count := len(mockStore.flushes)
	mockStore.mu.Unlock()
	assert.GreaterOrEqual(t, count, 1)
}

func TestStreamConsumer_DLQ(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	failStore := &FailingEventStore{
		failErr: errors.New("simulated poison pill"),
	}

	producer := NewStreamProducer(rdb, "s_dlq", 1000, 1*time.Second)

	proc := NewStreamConsumer(failStore, rdb, "s_dlq", "g_dlq", "c_dlq", 2, 1, 10*time.Millisecond, 1*time.Second, 10*time.Millisecond, 10*time.Millisecond, 1, 1*time.Minute, 1*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proc.Start(ctx)

	for i := 0; i < 2; i++ {
		_ = producer.Process(&domain.Event{CampaignID: uuid.New(), Type: "click"})
	}

	assert.Eventually(t, func() bool {
		size, err := rdb.XLen(ctx, "ad:events:dlq").Result()
		return err == nil && size == 2
	}, 3*time.Second, 50*time.Millisecond, "Should have 2 events in DLQ")

	pending, err := rdb.XPending(ctx, "s_dlq", "g_dlq").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count)

	proc.Close()
	proc.Wait(ctx)
}
