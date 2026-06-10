package ads

import (
	"sync"
	"time"
)

// Zero value is CircuitClosed (intentional).
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

// failThreshold is per workerID, not global.
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
