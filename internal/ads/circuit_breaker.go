package ads

import (
	"sync"
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
	failures      sync.Map // string -> *atomic.Int32
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

func (cb *CircuitBreaker) RecordSuccess(workerID string) {
	cb.failures.Range(func(key, value interface{}) bool {
		value.(*atomic.Int32).Store(0)
		return true
	})
	cb.state.Store(int32(CircuitClosed))
}

func (cb *CircuitBreaker) RecordFailure(workerID string) {
	val, _ := cb.failures.LoadOrStore(workerID, &atomic.Int32{})
	counter := val.(*atomic.Int32)
	newCount := counter.Add(1)

	if newCount >= cb.failThreshold {
		if oldState := CircuitState(cb.state.Swap(int32(CircuitOpen))); oldState != CircuitOpen {
			cb.lastOpenedAt.Store(time.Now().UnixNano())
		}
	}
}

func (cb *CircuitBreaker) State() CircuitState {
	return CircuitState(cb.state.Load())
}

func (cb *CircuitBreaker) Failures(workerID string) int {
	if val, ok := cb.failures.Load(workerID); ok {
		return int(val.(*atomic.Int32).Load())
	}
	return 0
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
