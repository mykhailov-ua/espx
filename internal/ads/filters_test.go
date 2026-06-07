package ads

import (
	"context"
	"testing"
	"time"

	"espx/internal/domain"
	"github.com/stretchr/testify/assert"
)

func TestIPRateLimiter(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	ctx := context.Background()
	limiter := NewIPRateLimiter(rdb, 3, 2*time.Second)

	evt1 := &domain.Event{IP: "192.168.1.1"}
	evt2 := &domain.Event{IP: "192.168.1.2"}

	assert.NoError(t, limiter.Check(ctx, evt1))
	assert.NoError(t, limiter.Check(ctx, evt1))
	assert.NoError(t, limiter.Check(ctx, evt1))

	assert.ErrorIs(t, limiter.Check(ctx, evt1), ErrRateLimitExceeded)
	assert.NoError(t, limiter.Check(ctx, evt2))

	time.Sleep(2500 * time.Millisecond)
	assert.NoError(t, limiter.Check(ctx, evt1))
}

func TestDuplicateEventFilter(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	ctx := context.Background()
	filter := NewDuplicateEventFilter(rdb, 1*time.Second)

	evt := &domain.Event{ClickID: "click_abc_123"}
	evtOther := &domain.Event{ClickID: "click_xyz_987"}

	assert.NoError(t, filter.Check(ctx, evt))
	assert.NoError(t, filter.Check(ctx, evtOther))

	assert.ErrorIs(t, filter.Check(ctx, evt), ErrDuplicateEvent)

	evtEmpty := &domain.Event{ClickID: ""}
	assert.NoError(t, filter.Check(ctx, evtEmpty))
	assert.NoError(t, filter.Check(ctx, evtEmpty))

	time.Sleep(1500 * time.Millisecond)
	assert.NoError(t, filter.Check(ctx, evt))
}

func TestFilterEngine(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	ctx := context.Background()
	limiter := NewIPRateLimiter(rdb, 3, 5*time.Second)
	dupFilter := NewDuplicateEventFilter(rdb, 5*time.Second)

	engine := NewFilterEngine(limiter, dupFilter)

	evt1 := &domain.Event{IP: "10.0.0.1", ClickID: "c_1"}
	evt2 := &domain.Event{IP: "10.0.0.1", ClickID: "c_2"}
	evt3 := &domain.Event{IP: "10.0.0.1", ClickID: "c_3"}

	assert.NoError(t, engine.Check(ctx, evt1))
	assert.ErrorIs(t, engine.Check(ctx, evt1), ErrDuplicateEvent)
	assert.NoError(t, engine.Check(ctx, evt2))
	assert.ErrorIs(t, engine.Check(ctx, evt3), ErrRateLimitExceeded)
}
