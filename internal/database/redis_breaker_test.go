package database

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisBreaker_StartsClosed(t *testing.T) {
	b := NewRedisBreaker(3, 2, 50*time.Millisecond)
	assert.Equal(t, CircuitClosed, b.State())
	assert.True(t, b.Allow())
}

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
