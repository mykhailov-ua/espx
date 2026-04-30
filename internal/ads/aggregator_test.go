package ads

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/repository"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockAggRepo struct {
	repository.Querier
	mock.Mock
}

func (m *MockAggRepo) UpdateCampaignStatsBatch(ctx context.Context, arg repository.UpdateCampaignStatsBatchParams) error {
	args := m.Called(ctx, arg)
	return args.Error(0)
}

func TestAggregator_TTL_Cleanup(t *testing.T) {
	// Set TTL to 1 second for the test
	origTTL := CampaignTTL
	CampaignTTL = 1 * time.Second
	defer func() { CampaignTTL = origTTL }()

	mockRepo := new(MockAggRepo)
	agg := NewAggregator(mockRepo, 10*time.Second, 1*time.Second, 1)

	id := uuid.New()
	agg.Increment(id, "click")

	// Verify it exists in memory
	val, ok := agg.data.Load(id)
	assert.True(t, ok)
	counters := val.(*Counters)

	// Manually age the campaign by setting LastSeen to 2 seconds ago
	counters.LastSeen.Store(time.Now().Add(-2 * time.Second).Unix())
	
	// Reset impressions/clicks/convs to 0 so it's eligible for deletion
	// (flush normally swaps them to 0 anyway)
	counters.Impressions.Store(0)
	counters.Clicks.Store(0)
	counters.Conversions.Store(0)

	// Trigger flush manually
	agg.flush()

	// Verify it was deleted from the map
	_, ok = agg.data.Load(id)
	assert.False(t, ok, "Campaign should have been removed from memory due to TTL")
}

func TestAggregator_Concurrency_Stress(t *testing.T) {
	mockRepo := new(MockAggRepo)
	// Track flushes
	var mu sync.Mutex
	totalImps := int64(0)
	
	mockRepo.On("UpdateCampaignStatsBatch", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		arg := args.Get(1).(repository.UpdateCampaignStatsBatchParams)
		mu.Lock()
		for _, v := range arg.Impressions {
			totalImps += v
		}
		mu.Unlock()
	}).Return(nil)

	agg := NewAggregator(mockRepo, 10*time.Millisecond, 1*time.Second, 4)
	ctx, cancel := context.WithCancel(context.Background())
	agg.Start(ctx)

	campaignID := uuid.New()
	const workers = 100
	const incsPerWorker = 1000
	expectedTotal := int64(workers * incsPerWorker)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < incsPerWorker; j++ {
				agg.Increment(campaignID, "impression")
			}
		}()
	}

	wg.Wait()
	time.Sleep(100 * time.Millisecond) // Wait for flush ticker
	
	cancel()
	agg.Stop()

	assert.Equal(t, expectedTotal, totalImps, "Aggregate impressions should match the total number of increments")
}
