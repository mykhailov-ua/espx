package database

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Connect(ctx context.Context, dsn string, maxConns, minConns int) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	config.MaxConns = int32(maxConns)
	config.MinConns = int32(minConns)
	config.MaxConnIdleTime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping db: %w", err)
	}

	// Parallel pings pre-establish physical connections up to minConns immediately.
	// This avoids lazy-loading TCP/TLS handshake latency penalties under initial high RPS traffic.
	if minConns > 1 {
		var wg sync.WaitGroup
		for i := 1; i < minConns; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = pool.Ping(ctx)
			}()
		}
		wg.Wait()
	}

	return pool, nil
}
