package ads

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	redis "github.com/redis/go-redis/v9"
)

var builderPool = sync.Pool{
	New: func() any {
		return &strings.Builder{}
	},
}

var (
	ErrRateLimitExceeded = errors.New("rate limit exceeded")
	ErrDuplicateEvent    = errors.New("duplicate event detected")
	ErrBudgetExhausted   = errors.New("budget exhausted")
	ErrCampaignNotFound  = errors.New("campaign not found in registry")
)

// BudgetFilter validates event costs against available campaign and customer balances.
// Chosen to prevent overspend before high-latency database synchronization occurs.
type BudgetFilter struct {
	manager          domain.BudgetManager
	registry         domain.CampaignRegistry
	clickAmount      float64
	impressionAmount float64
}

// NewBudgetFilter initializes a budget validator with specific pricing for event types.
func NewBudgetFilter(manager domain.BudgetManager, registry domain.CampaignRegistry, clickAmount, impressionAmount float64) *BudgetFilter {
	return &BudgetFilter{
		manager:          manager,
		registry:         registry,
		clickAmount:      clickAmount,
		impressionAmount: impressionAmount,
	}
}

// Check verifies budget availability and records the tentative spend in the cache.
func (f *BudgetFilter) Check(ctx context.Context, evt *domain.Event) error {
	customerID, ok := f.registry.GetCustomerID(evt.CampaignID)
	if !ok {
		return errors.New("campaign not found in registry")
	}

	amount := f.clickAmount
	if evt.Type == "impression" {
		amount = f.impressionAmount
	}

	allowed, err := f.manager.CheckAndSpend(ctx, customerID, evt.CampaignID, evt.ClickID, amount)
	if err != nil {
		return err
	}
	if !allowed {
		return ErrBudgetExhausted
	}
	return nil
}

// EventFilter defines the common interface for all sequential event validations.
type EventFilter interface {
	Check(ctx context.Context, evt *domain.Event) error
}

// FilterEngine executes a sequence of filters and returns the first error encountered.
// Chosen to provide a flexible and extensible validation pipeline for incoming events.
type FilterEngine struct {
	filters []EventFilter
}

// NewFilterEngine constructs a pipeline from the provided filter set.
func NewFilterEngine(filters ...EventFilter) *FilterEngine {
	return &FilterEngine{filters: filters}
}

// Check iterates through registered filters and halts processing on the first violation.
func (e *FilterEngine) Check(ctx context.Context, evt *domain.Event) error {
	for _, f := range e.filters {
		if err := f.Check(ctx, evt); err != nil {
			return err
		}
	}
	return nil
}

// IPRateLimiter prevents flood attacks by tracking request counts per IP address in Redis.
// Chosen for its distributed efficiency and atomic increments using Lua scripts.
type IPRateLimiter struct {
	rdb    redis.Cmdable
	limit  int
	window time.Duration
}

// NewIPRateLimiter initializes a rate limiter with a specific request quota and time window.
func NewIPRateLimiter(rdb redis.Cmdable, limit int, window time.Duration) *IPRateLimiter {
	return &IPRateLimiter{
		rdb:    rdb,
		limit:  limit,
		window: window,
	}
}

const rateLimitScript = `
local current = redis.call("INCR", KEYS[1])
if current == 1 then
    redis.call("PEXPIRE", KEYS[1], ARGV[1])
end
if current > tonumber(ARGV[2]) then
    return 1 -- limit exceeded
end
return 0 -- allowed
`

// Check evaluates the current rate limit for the client IP and increments the counter.
func (l *IPRateLimiter) Check(ctx context.Context, evt *domain.Event) error {
	if evt.IP == "" {
		return nil
	}

	sb := builderPool.Get().(*strings.Builder)
	sb.Reset()
	sb.Grow(13 + len(evt.IP))
	sb.WriteString("ratelimit:ip:")
	sb.WriteString(evt.IP)
	key := sb.String()
	builderPool.Put(sb)

	windowMs := int64(l.window.Milliseconds())

	res, err := l.rdb.Eval(ctx, rateLimitScript, []string{key}, windowMs, l.limit).Result()
	if err != nil {
		return nil
	}

	if res.(int64) == 1 {
		return ErrRateLimitExceeded
	}

	return nil
}

// DuplicateEventFilter prevents double-processing of events within a short TTL window.
// Chosen as a first-line defense to minimize database contention on unique constraints.
type DuplicateEventFilter struct {
	rdb redis.Cmdable
	ttl time.Duration
}

// NewDuplicateEventFilter initializes a deduplication filter with a specific expiration TTL.
func NewDuplicateEventFilter(rdb redis.Cmdable, ttl time.Duration) *DuplicateEventFilter {
	return &DuplicateEventFilter{
		rdb: rdb,
		ttl: ttl,
	}
}

// Check uses atomic SetNX to ensure an event ID is only processed once within the TTL.
func (f *DuplicateEventFilter) Check(ctx context.Context, evt *domain.Event) error {
	if evt.ClickID == "" {
		return nil
	}

	sb := builderPool.Get().(*strings.Builder)
	sb.Reset()
	sb.Grow(5 + len(evt.Type) + len(evt.ClickID))
	sb.WriteString("dup:")
	sb.WriteString(evt.Type)
	sb.WriteByte(':')
	sb.WriteString(evt.ClickID)
	key := sb.String()
	builderPool.Put(sb)

	ok, err := f.rdb.SetNX(ctx, key, "1", f.ttl).Result()
	if err != nil {
		return nil
	}

	if !ok {
		return ErrDuplicateEvent
	}

	return nil
}
