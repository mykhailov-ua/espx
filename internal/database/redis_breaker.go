// Package database provides a lock-free Redis circuit breaker implemented with
// sync/atomic operations. Unlike the mutex-based ads.CircuitBreaker, this breaker
// uses CAS (CompareAndSwap) for state transitions to avoid lock contention on the
// critical Redis command path.
//
// The breaker is installed as a redis.UniversalClient hook via RedisCircuitBreakerHook.
// The hook intercepts ProcessHook and ProcessPipelineHook; transport errors
// (network, EOF, connection refused) increment the failure counter while
// redis.Nil and business-logic errors are treated as successes.
//
// IsNetworkOrSystemError classifies errors by type (net.Error, context.Canceled)
// and by string-pattern matching for error types not wrapped in a net.Error.
package database

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"time"

	redis "github.com/redis/go-redis/v9"
)

var ErrRedisCircuitOpen = errors.New("redis circuit breaker is open")

type CircuitState int32

const (
	CircuitClosed   CircuitState = 0
	CircuitOpen     CircuitState = 1
	CircuitHalfOpen CircuitState = 2
)

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

// RedisBreaker is a lock-free circuit breaker for Redis clients. All fields are
// accessed exclusively via sync/atomic; no mutex is held during Allow, RecordSuccess,
// or RecordFailure. State transitions from Open -> HalfOpen use CompareAndSwap to
// prevent multiple goroutines from entering HalfOpen simultaneously.
type RedisBreaker struct {
	state            int32
	failures         int64
	successes        int64
	lastOpenedUnix   int64
	failThreshold    int64
	successThreshold int64
	openTimeout      time.Duration
}

func NewRedisBreaker(failThreshold, successThreshold int64, openTimeout time.Duration) *RedisBreaker {
	return &RedisBreaker{
		state:            int32(CircuitClosed),
		failThreshold:    failThreshold,
		successThreshold: successThreshold,
		openTimeout:      openTimeout,
	}
}

func (b *RedisBreaker) State() CircuitState {
	return CircuitState(atomic.LoadInt32(&b.state))
}

// Allow returns true if the breaker permits a Redis operation. In the Open state,
// it tests whether openTimeout has elapsed and if so performs a CAS to transition
// to HalfOpen, returning true only for the single goroutine that wins the CAS.
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

// RedisCircuitBreakerHook implements redis.Hook and injects breaker logic into
// every command dispatched through the client. It must be added via client.AddHook
// before any commands are issued.
type RedisCircuitBreakerHook struct {
	breaker *RedisBreaker
}

func NewRedisCircuitBreakerHook(breaker *RedisBreaker) *RedisCircuitBreakerHook {
	return &RedisCircuitBreakerHook{breaker: breaker}
}

func (h *RedisCircuitBreakerHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *RedisCircuitBreakerHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if !h.breaker.Allow() {
			cmd.SetErr(ErrRedisCircuitOpen)
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
		return err
	}
}

func (h *RedisCircuitBreakerHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if !h.breaker.Allow() {
			for _, cmd := range cmds {
				cmd.SetErr(ErrRedisCircuitOpen)
			}
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
		return err
	}
}
