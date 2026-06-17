package management

import (
	"context"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/database"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBlockIPUsesOutbox guards BlockIP enqueues outbox work before Redis reflects the block.
func TestBlockIPUsesOutbox(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	svc := NewService(pool, []redis.UniversalClient{rdb}, ads.NewJumpHashSharder(1), nil)
	defer svc.Close()

	ctx := context.Background()
	require.NoError(t, svc.BlockIP(ctx, "10.0.0.1", "fraud"))

	var outboxCount int
	err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM outbox_events
		WHERE event_type = 'UPDATE_BLACKLIST' AND status IN ('PENDING', 'PROCESSING')`).Scan(&outboxCount)
	require.NoError(t, err)
	assert.Equal(t, 1, outboxCount)

	assert.Eventually(t, func() bool {
		isMember, err := rdb.SIsMember(ctx, "blacklist:fraud", "10.0.0.1").Result()
		return err == nil && isMember
	}, 2*time.Second, 20*time.Millisecond)
}
