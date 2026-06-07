package ads

import (
	"context"
	"testing"
	"time"

	"espx/internal/domain"
	"github.com/google/uuid"
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

func (m *mockCampaignRepo) UpdateSpend(ctx context.Context, id uuid.UUID, amount int64, txID string) error {
	args := m.Called(ctx, id, amount, txID)
	return args.Error(0)
}

func (m *mockCampaignRepo) ListActive(ctx context.Context) ([]*domain.Campaign, error) {
	args := m.Called(ctx)
	return args.Get(0).([]*domain.Campaign), args.Error(1)
}

func TestMultiShardBudgetSync(t *testing.T) {
	ctx := context.Background()

	rdb1 := redis.NewClient(&redis.Options{Addr: "localhost:6479"})
	rdb2 := redis.NewClient(&redis.Options{Addr: "localhost:6480"})

	if err := rdb1.Ping(ctx).Err(); err != nil {
		t.Skip("Redis shard 1 not available")
	}
	if err := rdb2.Ping(ctx).Err(); err != nil {
		t.Skip("Redis shard 2 not available")
	}

	campaignID := uuid.New()

	rdb1.SAdd(ctx, "budget:dirty_campaigns", campaignID.String())
	rdb1.Set(ctx, "budget:sync:campaign:"+campaignID.String(), 10500000, 0)

	rdb2.SAdd(ctx, "budget:dirty_campaigns", campaignID.String())
	rdb2.Set(ctx, "budget:sync:campaign:"+campaignID.String(), 5250000, 0)

	repo := new(mockCampaignRepo)

	repo.On("UpdateSpend", mock.Anything, campaignID, int64(10500000), mock.Anything).Return(nil).Once()
	repo.On("UpdateSpend", mock.Anything, campaignID, int64(5250000), mock.Anything).Return(nil).Once()

	worker1 := NewSyncWorker(rdb1, repo, nil, 100*time.Millisecond)
	worker2 := NewSyncWorker(rdb2, repo, nil, 100*time.Millisecond)

	worker1.SyncAll(ctx)
	worker2.SyncAll(ctx)

	repo.AssertExpectations(t)

	val1, _ := rdb1.Get(ctx, "budget:sync:campaign:"+campaignID.String()).Result()
	assert.Empty(t, val1)

	val2, _ := rdb2.Get(ctx, "budget:sync:campaign:"+campaignID.String()).Result()
	assert.Empty(t, val2)
}
