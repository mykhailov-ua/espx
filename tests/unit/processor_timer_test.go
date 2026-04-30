package unit

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/repository"
	"github.com/stretchr/testify/assert"
)

type MockTimerQuerier struct {
	repository.Querier
	mu      sync.Mutex
	flushed bool
}

func (m *MockTimerQuerier) InsertEventsBatch(ctx context.Context, arg repository.InsertEventsBatchParams) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushed = true
	return nil
}

func TestProcessor_FlushByTicker(t *testing.T) {
	mockRepo := new(MockTimerQuerier)
	// Short flush interval: 50ms
	// Large batch size: 100 (so it won't flush by size)
	proc := ads.NewProcessor(mockRepo, 100, 1, 50*time.Millisecond, 1*time.Second)
	proc.Start(context.Background())
	defer proc.Close()

	err := proc.Process(ads.Event{CampaignID: uuid.New(), Type: "click"})
	assert.NoError(t, err)

	// Initially should not be flushed
	mockRepo.mu.Lock()
	assert.False(t, mockRepo.flushed)
	mockRepo.mu.Unlock()

	// Wait for more than the flush interval
	assert.Eventually(t, func() bool {
		mockRepo.mu.Lock()
		defer mockRepo.mu.Unlock()
		return mockRepo.flushed
	}, 200*time.Millisecond, 10*time.Millisecond, "Should have flushed by ticker")
}
