package database

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRedisBreaker_StartsClosed guards a new breaker accepts traffic before any failures are recorded.
func TestRedisBreaker_StartsClosed(t *testing.T) {
	b := NewRedisBreaker(3, 2, 50*time.Millisecond)
	assert.Equal(t, CircuitClosed, b.State())
	assert.True(t, b.Allow())
}

// TestRedisBreaker_TripsAfterThreshold guards consecutive transport failures open the circuit at the configured limit.
func TestRedisBreaker_TripsAfterThreshold(t *testing.T) {
	b := NewRedisBreaker(3, 2, 50*time.Millisecond)

	b.RecordFailure()
	b.RecordFailure()
	assert.Equal(t, CircuitClosed, b.State())
	assert.True(t, b.Allow())

	b.RecordFailure()
	assert.Equal(t, CircuitOpen, b.State())
	assert.False(t, b.Allow())
}

// TestRedisBreaker_TransitionsToHalfOpen guards the breaker admits probe traffic after the open timeout elapses.
func TestRedisBreaker_TransitionsToHalfOpen(t *testing.T) {
	b := NewRedisBreaker(1, 2, 20*time.Millisecond)

	b.RecordFailure()
	assert.Equal(t, CircuitOpen, b.State())
	assert.False(t, b.Allow())

	time.Sleep(25 * time.Millisecond)

	assert.True(t, b.Allow())
	assert.Equal(t, CircuitHalfOpen, b.State())

	assert.True(t, b.Allow())
}

// TestRedisBreaker_HalfOpenFailureReopens guards a failed probe immediately reopens the circuit.
func TestRedisBreaker_HalfOpenFailureReopens(t *testing.T) {
	b := NewRedisBreaker(1, 2, 20*time.Millisecond)

	b.RecordFailure()
	assert.Equal(t, CircuitOpen, b.State())

	time.Sleep(25 * time.Millisecond)
	require.True(t, b.Allow())

	b.RecordFailure()
	assert.Equal(t, CircuitOpen, b.State())
	assert.False(t, b.Allow())
}

// TestRedisBreaker_HalfOpenSuccessCloses guards enough probe successes close the circuit again.
func TestRedisBreaker_HalfOpenSuccessCloses(t *testing.T) {
	b := NewRedisBreaker(1, 2, 20*time.Millisecond)

	b.RecordFailure()
	assert.Equal(t, CircuitOpen, b.State())

	time.Sleep(25 * time.Millisecond)
	require.True(t, b.Allow())

	b.RecordSuccess()
	assert.Equal(t, CircuitHalfOpen, b.State())

	require.True(t, b.Allow())
	b.RecordSuccess()
	assert.Equal(t, CircuitClosed, b.State())
	assert.True(t, b.Allow())
}

// TestIsNetworkOrSystemError guards only infrastructure failures trip the breaker, not Redis business errors.
func TestIsNetworkOrSystemError(t *testing.T) {
	assert.False(t, IsNetworkOrSystemError(nil))
	assert.False(t, IsNetworkOrSystemError(redis.Nil))
	assert.False(t, IsNetworkOrSystemError(errors.New("ERR syntax error")))

	assert.True(t, IsNetworkOrSystemError(context.DeadlineExceeded))
	assert.True(t, IsNetworkOrSystemError(context.Canceled))
	assert.True(t, IsNetworkOrSystemError(errors.New("connection refused")))
	assert.True(t, IsNetworkOrSystemError(errors.New("broken pipe")))
	assert.True(t, IsNetworkOrSystemError(errors.New("client is closed")))

	netErr := &net.DNSError{IsTimeout: true}
	assert.True(t, IsNetworkOrSystemError(netErr))
}

// TestRedisBreaker_ConcurrentStress guards concurrent Allow and Record calls never corrupt breaker state.
func TestRedisBreaker_ConcurrentStress(t *testing.T) {
	b := NewRedisBreaker(100, 2, 10*time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			b.Allow()
			if idx%2 == 0 {
				b.RecordSuccess()
			} else {
				b.RecordFailure()
			}
		}(i)
	}
	wg.Wait()

	state := b.State()
	assert.Contains(t, []CircuitState{CircuitClosed, CircuitOpen}, state)
}

// TestRedisBreaker_FastFailWhenOpen guards an open breaker rejects commands without hitting Redis again.
func TestRedisBreaker_FastFailWhenOpen(t *testing.T) {
	const threshold = 5
	b := NewRedisBreaker(threshold, 2, time.Minute)
	hook := NewRedisCircuitBreakerHook(b, "0")

	var redisCalls atomic.Int64
	next := func(ctx context.Context, cmd redis.Cmder) error {
		redisCalls.Add(1)
		return errors.New("connection refused")
	}
	processHook := hook.ProcessHook(next)

	for i := 0; i < threshold; i++ {
		cmd := redis.NewStatusCmd(context.Background(), "PING")
		err := processHook(context.Background(), cmd)
		require.Error(t, err)
	}
	require.Equal(t, CircuitOpen, b.State())
	callsAtOpen := redisCalls.Load()

	for i := 0; i < 100; i++ {
		cmd := redis.NewStatusCmd(context.Background(), "PING")
		err := processHook(context.Background(), cmd)
		require.ErrorIs(t, err, ErrRedisCircuitOpen)
		require.ErrorIs(t, cmd.Err(), ErrRedisCircuitOpen)
	}

	require.Equal(t, callsAtOpen, redisCalls.Load(), "open breaker must fast-fail without hitting redis")
}
