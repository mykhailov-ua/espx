package ads

import (
	"sync/atomic"
	"time"
)

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

type CircuitBreaker struct {
	state         atomic.Int32
	failures      atomic.Int32
	lastOpenedAt  atomic.Int64
	failThreshold int32
	openTimeout   time.Duration
}

func NewCircuitBreaker(failThreshold int, openTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		failThreshold: int32(failThreshold),
		openTimeout:   openTimeout,
	}
}

func (cb *CircuitBreaker) Allow() bool {
	state := CircuitState(cb.state.Load())

	switch state {
	case CircuitClosed:
		return true

	case CircuitOpen:
		openedAt := cb.lastOpenedAt.Load()
		if time.Since(time.Unix(0, openedAt)) >= cb.openTimeout {
			// Atomically transition to HalfOpen to ensure only a single probe request is allowed.
			if cb.state.CompareAndSwap(int32(CircuitOpen), int32(CircuitHalfOpen)) {
				return true
			}
		}
		return false

	case CircuitHalfOpen:
		return false

	default:
		return true
	}
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.failures.Store(0)
	cb.state.Store(int32(CircuitClosed))
}

func (cb *CircuitBreaker) RecordFailure() {
	newCount := cb.failures.Add(1)

	if newCount >= cb.failThreshold {
		// Use Swap to update lastOpenedAt only on the initial state transition to avoid sliding window resets.
		if oldState := CircuitState(cb.state.Swap(int32(CircuitOpen))); oldState != CircuitOpen {
			cb.lastOpenedAt.Store(time.Now().UnixNano())
		}
	}
}

func (cb *CircuitBreaker) State() CircuitState {
	return CircuitState(cb.state.Load())
}

func (cb *CircuitBreaker) Failures() int {
	return int(cb.failures.Load())
}

func (cb *CircuitBreaker) WaitDuration() time.Duration {
	if CircuitState(cb.state.Load()) != CircuitOpen {
		return 0
	}
	openedAt := time.Unix(0, cb.lastOpenedAt.Load())
	remaining := cb.openTimeout - time.Since(openedAt)
	if remaining < 0 {
		return 0
	}
	return remaining
}
