package ads

import ()

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"espx/internal/config"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

type mockPinger struct {
	fail bool
}

func (m *mockPinger) Ping(ctx context.Context) error {
	if m.fail {
		return errors.New("ping failed")
	}
	return nil
}

type mockFailRedis struct {
	redis.UniversalClient
	fail bool
}

func (m *mockFailRedis) Ping(ctx context.Context) *redis.StatusCmd {
	cmd := redis.NewStatusCmd(ctx)
	if m.fail {
		cmd.SetErr(errors.New("redis connection refused"))
	} else {
		cmd.SetVal("PONG")
	}
	return cmd
}

func TestHealthCheckPartialFailure(t *testing.T) {
	cfg := &config.Config{}
	registry := &mockRegistry{}

	t.Run("All Healthy", func(t *testing.T) {
		rdbs := []redis.UniversalClient{
			&mockFailRedis{fail: false},
		}
		pool := &mockPinger{fail: false}
		sharder := NewJumpHashSharder(1)
		handler := NewRouter(cfg, registry, nil, pool, rdbs, sharder, "fraud-stream")

		req := httptest.NewRequest("GET", "/health", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "OK", w.Body.String())
	})

	t.Run("Postgres Down", func(t *testing.T) {
		rdbs := []redis.UniversalClient{
			&mockFailRedis{fail: false},
		}
		pool := &mockPinger{fail: true}
		sharder := NewJumpHashSharder(1)
		handler := NewRouter(cfg, registry, nil, pool, rdbs, sharder, "fraud-stream")

		req := httptest.NewRequest("GET", "/health", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
		assert.Contains(t, w.Body.String(), "postgres unreachable")
	})

	t.Run("Redis Shard 2 Down", func(t *testing.T) {
		rdbs := []redis.UniversalClient{
			&mockFailRedis{fail: false},
			&mockFailRedis{fail: true},
		}
		pool := &mockPinger{fail: false}
		sharder := NewJumpHashSharder(1)
		handler := NewRouter(cfg, registry, nil, pool, rdbs, sharder, "fraud-stream")

		req := httptest.NewRequest("GET", "/health", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
		assert.Contains(t, w.Body.String(), "redis shard unreachable")
	})
}
