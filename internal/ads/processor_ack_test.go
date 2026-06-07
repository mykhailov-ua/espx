package ads

import (
	"context"
	"errors"
	"testing"
	"time"

	"espx/internal/domain"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

type mockRedisClientAck struct {
	redis.UniversalClient
	xAckFunc func(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd
}

func (m *mockRedisClientAck) XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
	if m.xAckFunc != nil {
		return m.xAckFunc(ctx, stream, group, ids...)
	}
	return redis.NewIntCmd(ctx)
}

func TestStreamConsumer_FlushBatch_XAckError(t *testing.T) {
	mockStore := &MockEventStore{}
	mockRdb := &mockRedisClientAck{
		xAckFunc: func(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
			cmd := redis.NewIntCmd(ctx)
			cmd.SetErr(errors.New("mock XAck error"))
			return cmd
		},
	}

	p := &StreamConsumer{
		store:        mockStore,
		rdb:          mockRdb,
		streamName:   "test-stream",
		groupName:    "test-group",
		writeTimeout: 10 * time.Second,
	}

	batch := []*domain.Event{{CampaignID: uuid.New(), Type: "click"}}
	msgIDs := []string{"1-0"}

	err := p.flushBatch(context.Background(), batch, msgIDs, "test-worker")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mock XAck error")

	mockStore.mu.Lock()
	flushesCount := len(mockStore.flushes)
	mockStore.mu.Unlock()
	assert.Equal(t, 1, flushesCount)
}
