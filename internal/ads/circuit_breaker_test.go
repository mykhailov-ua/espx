package ads

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCircuitBreaker_StartsInClosedState(t *testing.T) {
	cb := NewCircuitBreaker(3, 1*time.Second)

	assert.Equal(t, CircuitClosed, cb.State())
	assert.True(t, cb.Allow())
	assert.Equal(t, 0, cb.Failures())
}

func TestCircuitBreaker_TripsAfterThreshold(t *testing.T) {
	cb := NewCircuitBreaker(3, 1*time.Second)

	cb.RecordFailure()
	assert.Equal(t, CircuitClosed, cb.State())
	assert.True(t, cb.Allow())

	cb.RecordFailure()
	assert.Equal(t, CircuitClosed, cb.State())
	assert.True(t, cb.Allow())

	cb.RecordFailure()
	assert.Equal(t, CircuitOpen, cb.State())
	assert.False(t, cb.Allow())
	assert.Equal(t, 3, cb.Failures())
}

func TestCircuitBreaker_SuccessResetsFailures(t *testing.T) {
	cb := NewCircuitBreaker(3, 1*time.Second)

	cb.RecordFailure()
	cb.RecordFailure()
	assert.Equal(t, 2, cb.Failures())

	cb.RecordSuccess()
	assert.Equal(t, CircuitClosed, cb.State())
	assert.Equal(t, 0, cb.Failures())
	assert.True(t, cb.Allow())
}

func TestCircuitBreaker_TransitionsToHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(2, 50*time.Millisecond)

	cb.RecordFailure()
	cb.RecordFailure()
	assert.Equal(t, CircuitOpen, cb.State())
	assert.False(t, cb.Allow())

	time.Sleep(60 * time.Millisecond)

	assert.True(t, cb.Allow())
	assert.Equal(t, CircuitHalfOpen, cb.State())
}

func TestCircuitBreaker_HalfOpenSuccessCloses(t *testing.T) {
	cb := NewCircuitBreaker(2, 50*time.Millisecond)

	cb.RecordFailure()
	cb.RecordFailure()
	assert.Equal(t, CircuitOpen, cb.State())

	time.Sleep(60 * time.Millisecond)

	require.True(t, cb.Allow()) // transitions to half-open
	cb.RecordSuccess()
	assert.Equal(t, CircuitClosed, cb.State())
	assert.True(t, cb.Allow())
}

func TestCircuitBreaker_HalfOpenFailureReopens(t *testing.T) {
	cb := NewCircuitBreaker(2, 50*time.Millisecond)

	cb.RecordFailure()
	cb.RecordFailure()
	assert.Equal(t, CircuitOpen, cb.State())

	time.Sleep(60 * time.Millisecond)

	require.True(t, cb.Allow()) // transitions to half-open
	cb.RecordFailure()
	assert.Equal(t, CircuitOpen, cb.State())
	assert.False(t, cb.Allow())
}

func TestCircuitBreaker_HalfOpenBlocksConcurrentProbes(t *testing.T) {
	cb := NewCircuitBreaker(1, 50*time.Millisecond)

	cb.RecordFailure()
	assert.Equal(t, CircuitOpen, cb.State())

	time.Sleep(60 * time.Millisecond)

	// First call wins the CAS and gets Allow()=true
	first := cb.Allow()
	assert.True(t, first)
	assert.Equal(t, CircuitHalfOpen, cb.State())

	// Subsequent calls in HalfOpen must return false
	assert.False(t, cb.Allow())
	assert.False(t, cb.Allow())
}

func TestCircuitBreaker_WaitDuration(t *testing.T) {
	cb := NewCircuitBreaker(1, 100*time.Millisecond)

	assert.Equal(t, time.Duration(0), cb.WaitDuration())

	cb.RecordFailure()
	assert.Equal(t, CircuitOpen, cb.State())

	d := cb.WaitDuration()
	assert.Greater(t, d, time.Duration(0))
	assert.LessOrEqual(t, d, 100*time.Millisecond)
}

func TestCircuitBreaker_StateString(t *testing.T) {
	assert.Equal(t, "closed", CircuitClosed.String())
	assert.Equal(t, "open", CircuitOpen.String())
	assert.Equal(t, "half-open", CircuitHalfOpen.String())
	assert.Equal(t, "unknown", CircuitState(99).String())
}

func TestCircuitBreaker_ConcurrentAccess(t *testing.T) {
	cb := NewCircuitBreaker(100, 50*time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cb.Allow()
			cb.RecordFailure()
			cb.Allow()
		}()
	}
	wg.Wait()

	// After 200 failures with threshold 100, must be Open.
	assert.Equal(t, CircuitOpen, cb.State())
	assert.False(t, cb.Allow())
}

func TestCircuitBreaker_ConcurrentMixedOps(t *testing.T) {
	cb := NewCircuitBreaker(50, 10*time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cb.Allow()
			if idx%3 == 0 {
				cb.RecordSuccess()
			} else {
				cb.RecordFailure()
			}
		}(i)
	}
	wg.Wait()

	// State is deterministic enough to verify it's one of the valid states.
	state := cb.State()
	assert.Contains(t, []CircuitState{CircuitClosed, CircuitOpen, CircuitHalfOpen}, state)
}
