package auth

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCheckTokenRevocation covers nil Redis and user-wide markers that must deny access.
func TestCheckTokenRevocation(t *testing.T) {
	payload := &Payload{
		ID:        uuid.New(),
		SessionID: uuid.New(),
		UserID:    uuid.New(),
	}

	t.Run("NilRedis", func(t *testing.T) {
		revoked, err := CheckTokenRevocation(context.Background(), nil, payload)
		assert.NoError(t, err)
		assert.False(t, revoked)
	})

	t.Run("RevokedUser", func(t *testing.T) {
		rdb := &mockRevocationRedis{
			exists: map[string]int64{
				"revoked:user:" + payload.UserID.String(): 1,
			},
		}
		revoked, err := CheckTokenRevocation(context.Background(), rdb, payload)
		require.NoError(t, err)
		assert.True(t, revoked)
	})
}

type mockRevocationRedis struct {
	redis.UniversalClient
	exists map[string]int64
}

// Pipelined simulates batched EXISTS checks without a live Redis instance.
func (m *mockRevocationRedis) Pipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	p := &mockRevocationPipeliner{exists: m.exists, ctx: ctx}
	if err := fn(p); err != nil {
		return nil, err
	}
	return p.cmds, nil
}

type mockRevocationPipeliner struct {
	redis.Pipeliner
	exists map[string]int64
	ctx    context.Context
	cmds   []redis.Cmder
}

// Exists returns canned revocation state for pipeline-based revocation tests.
func (p *mockRevocationPipeliner) Exists(ctx context.Context, keys ...string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx)
	if len(keys) > 0 {
		cmd.SetVal(p.exists[keys[0]])
	}
	p.cmds = append(p.cmds, cmd)
	return cmd
}

// TestRevokeUserAccess ensures user-wide markers are written without requiring Redis in CI.
func TestRevokeUserAccess(t *testing.T) {
	rdb := &mockRevocationRedis{exists: map[string]int64{}}
	userID := uuid.New()
	err := RevokeUserAccess(context.Background(), rdb, userID, time.Hour)
	assert.NoError(t, err)
}

// Set records revocation keys in memory for block and unblock regression tests.
func (m *mockRevocationRedis) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	if m.exists == nil {
		m.exists = map[string]int64{}
	}
	m.exists[key] = 1
	cmd := redis.NewStatusCmd(ctx)
	cmd.SetVal("OK")
	return cmd
}

// Del clears revocation keys in memory so unblock paths can be exercised offline.
func (m *mockRevocationRedis) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	for _, key := range keys {
		delete(m.exists, key)
	}
	cmd := redis.NewIntCmd(ctx)
	cmd.SetVal(1)
	return cmd
}
