package database

import (
	"context"
	"fmt"

	redis "github.com/redis/go-redis/v9"
)

// ConnectRedis dials a Redis shard for services that do not need per-command circuit breaking.
func ConnectRedis(ctx context.Context, addr string, password string) (redis.UniversalClient, error) {
	return ConnectRedisWithBreaker(ctx, addr, password, nil)
}

// ConnectRedisWithBreaker dials Redis and optionally installs a breaker hook so unhealthy shards fail fast on the hot path.
func ConnectRedisWithBreaker(ctx context.Context, addr string, password string, breaker *RedisBreaker) (redis.UniversalClient, error) {
	rdb := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    []string{addr},
		Password: password,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	if breaker != nil {
		rdb.AddHook(NewRedisCircuitBreakerHook(breaker, "0"))
	}

	return rdb, nil
}
