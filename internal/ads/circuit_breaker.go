package ads

import (
	"sync/atomic"
	"time"
)

// CircuitState represents the state of the circuit breaker.
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

// CircuitBreaker implements a lock-free state machine that stops
// downstream calls when the target (Postgres/ClickHouse) is unavailable.
//
// State transitions:
//
//	Closed  -> Open      : after failThreshold consecutive failures
//	Open    -> HalfOpen  : after openTimeout elapses
//	HalfOpen -> Closed   : on first success (probe passed)
//	HalfOpen -> Open     : on first failure (probe failed, reset timer)
type CircuitBreaker struct {
	state         atomic.Int32
	failures      atomic.Int32
	lastOpenedAt  atomic.Int64 // unix nanoseconds
	failThreshold int32
	openTimeout   time.Duration
}

// NewCircuitBreaker creates a circuit breaker.
// failThreshold: consecutive failures before tripping to Open.
// openTimeout: duration to wait in Open before allowing a probe.
func NewCircuitBreaker(failThreshold int, openTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		failThreshold: int32(failThreshold),
		openTimeout:   openTimeout,
	}
}

// Allow checks whether a request is allowed to proceed.
// Returns true if the circuit is Closed or if the Open timeout has
// elapsed (transitioning to HalfOpen for a probe attempt).
func (cb *CircuitBreaker) Allow() bool {
	state := CircuitState(cb.state.Load())

	switch state {
	case CircuitClosed:
		return true

	case CircuitOpen:
		openedAt := cb.lastOpenedAt.Load()
		if time.Since(time.Unix(0, openedAt)) >= cb.openTimeout {
			// Attempt transition to HalfOpen. Only one goroutine wins the CAS.
			if cb.state.CompareAndSwap(int32(CircuitOpen), int32(CircuitHalfOpen)) {
				return true
			}
			// Another goroutine already transitioned; re-check.
			return CircuitState(cb.state.Load()) == CircuitHalfOpen
		}
		return false

	case CircuitHalfOpen:
		// Only the probe goroutine (the one that won the CAS) should proceed.
		// All others block until the probe resolves.
		return false

	default:
		return true
	}
}

// RecordSuccess signals a successful operation. Resets failure count
// and transitions HalfOpen -> Closed.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.failures.Store(0)
	cb.state.Store(int32(CircuitClosed))
}

// RecordFailure signals a failed operation. Increments the failure
// counter and trips the breaker to Open when the threshold is exceeded.
func (cb *CircuitBreaker) RecordFailure() {
	newCount := cb.failures.Add(1)

	if newCount >= cb.failThreshold {
		// Trip to Open.
		cb.state.Store(int32(CircuitOpen))
		cb.lastOpenedAt.Store(time.Now().UnixNano())
	}
}

// State returns the current circuit state.
func (cb *CircuitBreaker) State() CircuitState {
	return CircuitState(cb.state.Load())
}

// Failures returns the current consecutive failure count.
func (cb *CircuitBreaker) Failures() int {
	return int(cb.failures.Load())
}

// WaitDuration returns how long a caller should sleep when the circuit
// is Open. Returns 0 if the circuit is not Open.
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
