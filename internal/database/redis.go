package database

import (
	"context"
	"fmt"

	redis "github.com/redis/go-redis/v9"
)

func ConnectRedis(ctx context.Context, addr string, password string) (redis.UniversalClient, error) {
	rdb := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    []string{addr},
		Password: password,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return rdb, nil
}
