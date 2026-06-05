// Package ads provides a mutex-guarded circuit breaker with per-worker failure
// counting. State transitions follow the standard three-state model:
//
//	Closed  -> (failure count >= threshold per worker)  -> Open
//	Open    -> (openTimeout elapsed, first Allow call)  -> HalfOpen
//	HalfOpen -> (RecordSuccess)  -> Closed
//	HalfOpen -> (RecordFailure or RecordCancellation)  -> Open
//
// Unlike the lock-free RedisBreaker in the database package, this implementation
// tracks failure counts at per-worker granularity to allow heterogeneous worker
// fleets (e.g., CH workers vs PG workers) sharing the same breaker to isolate
// noise sources before tripping. Only the mutex-guarded failure map is authoritative;
// the state field is protected by the same lock.
package ads

import (
	"sync"
	"time"
)

// CircuitState is the type-safe enumeration of circuit breaker states. The zero
// value (CircuitClosed = 0) is intentional: a freshly zeroed CircuitBreaker struct
// begins in the closed (allowing) state without requiring explicit initialisation.
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

// CircuitBreaker gates downstream calls for the StreamConsumer worker pool.
// failThreshold is evaluated per workerID key; opening one worker's counter does not
// immediately trip workers whose individual counts are below threshold.
type CircuitBreaker struct {
	mu            sync.Mutex
	state         CircuitState
	failures      map[string]int32
	lastOpenedAt  time.Time
	failThreshold int32
	openTimeout   time.Duration
}

func NewCircuitBreaker(failThreshold int, openTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:         CircuitClosed,
		failures:      make(map[string]int32),
		failThreshold: int32(failThreshold),
		openTimeout:   openTimeout,
	}
}

// Allow returns true if the breaker permits the caller to attempt a downstream
// operation. In the Open state it returns true only once openTimeout has elapsed,
// simultaneously transitioning to HalfOpen to probe recovery. Concurrent callers
// during the Open->HalfOpen transition all read the same state change because the
// state assignment and Allow check share the same mutex.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return true

	case CircuitOpen:
		if time.Since(cb.lastOpenedAt) >= cb.openTimeout {
			cb.state = CircuitHalfOpen
			return true
		}
		return false

	case CircuitHalfOpen:
		return false

	default:
		return true
	}
}

// RecordSuccess resets per-worker failure counts and, when in HalfOpen state,
// promotes the breaker back to Closed.
func (cb *CircuitBreaker) RecordSuccess(workerID string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == CircuitHalfOpen {
		cb.failures = make(map[string]int32)
		cb.state = CircuitClosed
	} else {
		delete(cb.failures, workerID)
	}
}

// RecordFailure increments the per-worker failure counter. If the count reaches
// failThreshold the breaker trips to Open; in HalfOpen the first failure immediately
// re-opens without checking the threshold.
func (cb *CircuitBreaker) RecordFailure(workerID string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures[workerID]++
	if cb.state == CircuitHalfOpen {
		cb.state = CircuitOpen
		cb.lastOpenedAt = time.Now()
		return
	}
	if cb.failures[workerID] >= cb.failThreshold {
		if cb.state != CircuitOpen {
			cb.state = CircuitOpen
			cb.lastOpenedAt = time.Now()
		}
	}
}

func (cb *CircuitBreaker) RecordCancellation(workerID string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == CircuitHalfOpen {
		cb.state = CircuitOpen
		cb.lastOpenedAt = time.Now()
	}
}

func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

func (cb *CircuitBreaker) Failures(workerID string) int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return int(cb.failures[workerID])
}

// WaitDuration returns the remaining time until the breaker may attempt HalfOpen.
// Returns 0 if the breaker is not in Open state. Used by workers to compute back-off
// sleep without polling Allow in a tight loop.
func (cb *CircuitBreaker) WaitDuration() time.Duration {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state != CircuitOpen {
		return 0
	}
	remaining := cb.openTimeout - time.Since(cb.lastOpenedAt)
	if remaining < 0 {
		return 0
	}
	return remaining
}
