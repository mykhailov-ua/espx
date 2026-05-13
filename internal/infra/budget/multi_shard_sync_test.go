package budget

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockCampaignRepo struct {
	mock.Mock
}

func (m *mockCampaignRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Campaign, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*domain.Campaign), args.Error(1)
}

func (m *mockCampaignRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.CampaignStatus) error {
	args := m.Called(ctx, id, status)
	return args.Error(0)
}

func (m *mockCampaignRepo) UpdateSpend(ctx context.Context, id uuid.UUID, amount float64) error {
	args := m.Called(ctx, id, amount)
	return args.Error(0)
}

func (m *mockCampaignRepo) ListActive(ctx context.Context) ([]*domain.Campaign, error) {
	args := m.Called(ctx)
	return args.Get(0).([]*domain.Campaign), args.Error(1)
}

func TestMultiShardBudgetSync(t *testing.T) {
	ctx := context.Background()

	// Initializes two independent Redis shards to simulate a distributed budget environment.

	rdb1 := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	rdb2 := redis.NewClient(&redis.Options{Addr: "localhost:6380"})

	// Ensure we can connect or skip
	if err := rdb1.Ping(ctx).Err(); err != nil {
		t.Skip("Redis shard 1 not available")
	}
	if err := rdb2.Ping(ctx).Err(); err != nil {
		t.Skip("Redis shard 2 not available")
	}

	campaignID := uuid.New()

	// Record spend events on both shards for the same campaign to simulate parallel ingestion.
	rdb1.SAdd(ctx, "budget:dirty_campaigns", campaignID.String())
	rdb1.Set(ctx, "budget:sync:campaign:"+campaignID.String(), 10.5, 0)

	rdb2.SAdd(ctx, "budget:dirty_campaigns", campaignID.String())
	rdb2.Set(ctx, "budget:sync:campaign:"+campaignID.String(), 5.25, 0)

	repo := new(mockCampaignRepo)

	// Configures expectations for atomic database updates per shard.
	repo.On("UpdateSpend", mock.Anything, campaignID, 10.5).Return(nil).Once()
	repo.On("UpdateSpend", mock.Anything, campaignID, 5.25).Return(nil).Once()

	worker1 := NewSyncWorker(rdb1, repo, nil, 100*time.Millisecond)
	worker2 := NewSyncWorker(rdb2, repo, nil, 100*time.Millisecond)

	// Executes reconciliation workers for all shards to move data from Redis to PostgreSQL.
	worker1.SyncAll(ctx)
	worker2.SyncAll(ctx)

	// Validates repository interactions and post-sync state consistency.
	repo.AssertExpectations(t)

	// Ensures that synchronization keys are purged from the Redis shards after successful persistence.
	val1, _ := rdb1.Get(ctx, "budget:sync:campaign:"+campaignID.String()).Result()
	assert.Empty(t, val1)

	val2, _ := rdb2.Get(ctx, "budget:sync:campaign:"+campaignID.String()).Result()
	assert.Empty(t, val2)
}
