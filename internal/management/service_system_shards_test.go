package management

import (
	"context"
	"testing"

	"espx/internal/database"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockIP_MultipleShards(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	rdb1, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	var endpoint string
	switch client := rdb1.(type) {
	case *redis.Client:
		endpoint = client.Options().Addr
	case *redis.ClusterClient:
		endpoint = client.Options().Addrs[0]
	default:
		t.Fatalf("unexpected redis client type")
	}

	rdb2 := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{endpoint},
		DB:    1,
	})
	defer rdb2.Close()

	rdb3 := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{endpoint},
		DB:    2,
	})
	defer rdb3.Close()

	svc := NewService(pool, []redis.UniversalClient{rdb1, rdb2, rdb3}, nil, nil)
	defer svc.Close()

	ctx := context.Background()
	testIP := "185.190.140.1"

	t.Run("BlockIP on all shards", func(t *testing.T) {
		err := svc.BlockIP(ctx, testIP, "manual")
		require.NoError(t, err)

		for i, rdb := range []redis.UniversalClient{rdb1, rdb2, rdb3} {
			isMember, err := rdb.SIsMember(ctx, "blacklist:manual", testIP).Result()
			require.NoError(t, err)
			assert.True(t, isMember, "IP should be blocked on shard %d", i+1)
		}
	})

	t.Run("UnblockIP on all shards", func(t *testing.T) {
		err := svc.UnblockIP(ctx, testIP, "manual")
		require.NoError(t, err)

		for i, rdb := range []redis.UniversalClient{rdb1, rdb2, rdb3} {
			isMember, err := rdb.SIsMember(ctx, "blacklist:manual", testIP).Result()
			require.NoError(t, err)
			assert.False(t, isMember, "IP should be unblocked on shard %d", i+1)
		}
	})

	t.Run("SyncSystemState on all shards", func(t *testing.T) {
		err := svc.BlockIP(ctx, "10.20.30.40", "auto")
		require.NoError(t, err)

		for _, rdb := range []redis.UniversalClient{rdb1, rdb2, rdb3} {
			rdb.Del(ctx, "blacklist:auto")
		}

		err = svc.SyncSystemState(ctx)
		require.NoError(t, err)

		for i, rdb := range []redis.UniversalClient{rdb1, rdb2, rdb3} {
			isMember, err := rdb.SIsMember(ctx, "blacklist:auto", "10.20.30.40").Result()
			require.NoError(t, err)
			assert.True(t, isMember, "IP should be synced on shard %d", i+1)
		}
	})
}
