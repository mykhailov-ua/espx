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

// CircuitBreaker implements a thread-safe state machine to prevent cascading failures.
// Chosen to protect downstream dependencies (e.g., PostgreSQL) from overload during partial outages.
type CircuitBreaker struct {
	state         atomic.Int32
	failures      atomic.Int32
	lastOpenedAt  atomic.Int64
	failThreshold int32
	openTimeout   time.Duration
}

// NewCircuitBreaker initializes the breaker with a failure threshold and reset timeout.
func NewCircuitBreaker(failThreshold int, openTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		failThreshold: int32(failThreshold),
		openTimeout:   openTimeout,
	}
}

// Allow determines if a request should be processed based on the current breaker state.
// Uses atomic CAS to safely transition from Open to HalfOpen for probing.
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

// RecordSuccess resets failure counts and returns the breaker to the Closed state.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.failures.Store(0)
	cb.state.Store(int32(CircuitClosed))
}

// RecordFailure increments the failure count and trips the breaker if the threshold is reached.
// Updates the timestamp only on transition to ensure a stable open window.
func (cb *CircuitBreaker) RecordFailure() {
	newCount := cb.failures.Add(1)

	if newCount >= cb.failThreshold {
		if oldState := CircuitState(cb.state.Swap(int32(CircuitOpen))); oldState != CircuitOpen {
			cb.lastOpenedAt.Store(time.Now().UnixNano())
		}
	}
}

// State returns the current internal state of the breaker.
func (cb *CircuitBreaker) State() CircuitState {
	return CircuitState(cb.state.Load())
}

// Failures returns the total number of consecutive failures recorded.
func (cb *CircuitBreaker) Failures() int {
	return int(cb.failures.Load())
}

// WaitDuration calculates the time remaining until the breaker can transition to HalfOpen.
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
