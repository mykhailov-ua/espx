package database

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"espx/internal/metrics"

	redis "github.com/redis/go-redis/v9"
)

// ErrRedisCircuitOpen signals the breaker rejected a command before it reached a failing shard.
var ErrRedisCircuitOpen = errors.New("redis circuit breaker is open")

// CircuitState is the observable breaker phase exported to metrics and logs.
type CircuitState int32

const (
	CircuitClosed   CircuitState = 0
	CircuitOpen     CircuitState = 1
	CircuitHalfOpen CircuitState = 2
)

// String maps breaker state to stable log and dashboard labels.
func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// RedisBreaker sheds Redis load when a shard is unhealthy so callers fail fast instead of piling up timeouts.
type RedisBreaker struct {
	state            int32
	failures         int64
	successes        int64
	lastOpenedUnix   int64
	failThreshold    int64
	successThreshold int64
	openTimeout      time.Duration
}

// NewRedisBreaker configures failure and recovery thresholds for a single Redis shard.
func NewRedisBreaker(failThreshold, successThreshold int64, openTimeout time.Duration) *RedisBreaker {
	return &RedisBreaker{
		state:            int32(CircuitClosed),
		failThreshold:    failThreshold,
		successThreshold: successThreshold,
		openTimeout:      openTimeout,
	}
}

// State returns the current breaker phase for hooks, metrics, and tests.
func (b *RedisBreaker) State() CircuitState {
	return CircuitState(atomic.LoadInt32(&b.state))
}

// Allow reports whether a Redis command may proceed or should fast-fail while the shard recovers.
func (b *RedisBreaker) Allow() bool {
	state := atomic.LoadInt32(&b.state)
	if state == int32(CircuitClosed) {
		return true
	}

	if state == int32(CircuitHalfOpen) {
		return true
	}

	if state == int32(CircuitOpen) {
		lastOpened := atomic.LoadInt64(&b.lastOpenedUnix)
		if time.Since(time.Unix(0, lastOpened)) >= b.openTimeout {

			if atomic.CompareAndSwapInt32(&b.state, int32(CircuitOpen), int32(CircuitHalfOpen)) {
				atomic.StoreInt64(&b.successes, 0)
				atomic.StoreInt64(&b.failures, 0)
				return true
			}
		}
		return false
	}

	return false
}

// RecordSuccess counts probe successes so the breaker can close after a shard proves healthy again.
func (b *RedisBreaker) RecordSuccess() {
	state := atomic.LoadInt32(&b.state)
	if state == int32(CircuitHalfOpen) {
		successes := atomic.AddInt64(&b.successes, 1)
		if successes >= b.successThreshold {
			if atomic.CompareAndSwapInt32(&b.state, int32(CircuitHalfOpen), int32(CircuitClosed)) {
				atomic.StoreInt64(&b.failures, 0)
			}
		}
	} else if state == int32(CircuitClosed) {

		atomic.StoreInt64(&b.failures, 0)
	}
}

// RecordFailure counts transport errors so the breaker opens before callers exhaust connection pools.
func (b *RedisBreaker) RecordFailure() {
	state := atomic.LoadInt32(&b.state)
	if state == int32(CircuitHalfOpen) {

		b.trip()
	} else if state == int32(CircuitClosed) {
		failures := atomic.AddInt64(&b.failures, 1)
		if failures >= b.failThreshold {
			b.trip()
		}
	}
}

// trip opens the breaker after repeated infrastructure failures.
func (b *RedisBreaker) trip() {
	for {
		state := atomic.LoadInt32(&b.state)
		if state == int32(CircuitOpen) {
			return
		}
		if atomic.CompareAndSwapInt32(&b.state, state, int32(CircuitOpen)) {
			atomic.StoreInt64(&b.lastOpenedUnix, time.Now().UnixNano())
			return
		}
	}
}

// IsNetworkOrSystemError separates infra outages from Redis business errors so the breaker only trips on real shard failures.
func IsNetworkOrSystemError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, redis.Nil) {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	errStr := err.Error()
	if strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "client is closed") ||
		strings.Contains(errStr, "use of closed network connection") {
		return true
	}
	return false
}

// RedisCircuitBreakerHook wraps go-redis commands with breaker gating and Prometheus state reporting.
type RedisCircuitBreakerHook struct {
	breaker *RedisBreaker
	shard   string
}

// NewRedisCircuitBreakerHook attaches shard-scoped breaker logic to a Redis client.
func NewRedisCircuitBreakerHook(breaker *RedisBreaker, shard string) *RedisCircuitBreakerHook {
	return &RedisCircuitBreakerHook{breaker: breaker, shard: shard}
}

// reportState publishes breaker phase to Prometheus for shard health dashboards.
func (h *RedisCircuitBreakerHook) reportState() {
	metrics.RedisBreakerState.WithLabelValues(h.shard).Set(float64(h.breaker.State()))
}

// DialHook passes through dial hooks because breaker decisions happen at command time.
func (h *RedisCircuitBreakerHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

// ProcessHook enforces breaker gating and outcome recording on single Redis commands.
func (h *RedisCircuitBreakerHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if !h.breaker.Allow() {
			cmd.SetErr(ErrRedisCircuitOpen)
			h.reportState()
			return ErrRedisCircuitOpen
		}

		err := next(ctx, cmd)
		if err != nil {
			if IsNetworkOrSystemError(err) {
				h.breaker.RecordFailure()
			} else {
				h.breaker.RecordSuccess()
			}
		} else {
			h.breaker.RecordSuccess()
		}
		h.reportState()
		return err
	}
}

// ProcessPipelineHook enforces breaker gating and outcome recording on pipelined Redis commands.
func (h *RedisCircuitBreakerHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if !h.breaker.Allow() {
			for _, cmd := range cmds {
				cmd.SetErr(ErrRedisCircuitOpen)
			}
			h.reportState()
			return ErrRedisCircuitOpen
		}

		err := next(ctx, cmds)
		if err != nil {
			if IsNetworkOrSystemError(err) {
				h.breaker.RecordFailure()
			} else {
				h.breaker.RecordSuccess()
			}
		} else {
			h.breaker.RecordSuccess()
		}
		h.reportState()
		return err
	}
}
