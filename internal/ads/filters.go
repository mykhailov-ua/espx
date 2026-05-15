package ads

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	ErrPacingExhausted   = errors.New("pacing exhausted")
	ErrFreqLimitExceeded = errors.New("frequency limit exceeded")
	ErrGeoBlocked        = errors.New("geo-targeting blocked")
	ErrFraudDetected     = errors.New("fraud detected")
)

// FraudFilter implements multi-layered bot detection including DC filtering and TTC analysis.
// Chosen to operate as an early-exit filter to prevent fraudulent spend and ingestion.
type FraudFilter struct {
	geo    GeoProvider
	rdb    redis.UniversalClient
	ttcMin time.Duration
}

func NewFraudFilter(geo GeoProvider, rdb redis.UniversalClient, ttcMin time.Duration) *FraudFilter {
	return &FraudFilter{
		geo:    geo,
		rdb:    rdb,
		ttcMin: ttcMin,
	}
}

func (f *FraudFilter) Check(ctx context.Context, evt *domain.Event) error {
	// 1. Data Center / Proxy Check (Stateless-ish)
	isAnon, err := f.geo.IsAnonymous(evt.IP)
	if err == nil && isAnon {
		evt.FraudReason = "datacenter_ip"
		return ErrFraudDetected
	}

	// 2. Time-to-Click (TTC) Analysis (Stateful)
	if evt.Type == "impression" {
		// Store impression timestamp for future click verification
		key := fmt.Sprintf("imp_ts:%s:%s", evt.UserID, evt.CampaignID)
		f.rdb.Set(ctx, key, time.Now().UnixMilli(), 10*time.Minute)
		return nil
	}

	if evt.Type == "click" {
		key := fmt.Sprintf("imp_ts:%s:%s", evt.UserID, evt.CampaignID)
		ts, err := f.rdb.Get(ctx, key).Int64()
		if err == nil {
			delta := time.Since(time.UnixMilli(ts))
			if delta < f.ttcMin {
				evt.FraudReason = fmt.Sprintf("fast_click:%v", delta)
				return ErrFraudDetected
			}
		}
		// Note: we allow clicks without an impression record (some environments lose impressions)
		// but log it if it's too frequent (future work).
	}

	return nil
}

// GeoFilter enforces regional targeting by matching client IP against campaign allowed countries.
// Chosen to operate as an early-exit stateless filter to minimize stateful Redis operations.
type GeoFilter struct {
	geo      GeoProvider
	registry domain.CampaignRegistry
}

func NewGeoFilter(geo GeoProvider, registry domain.CampaignRegistry) *GeoFilter {
	return &GeoFilter{
		geo:      geo,
		registry: registry,
	}
}

func (f *GeoFilter) Check(ctx context.Context, evt *domain.Event) error {
	camp, ok := f.registry.GetCampaign(evt.CampaignID)
	if !ok {
		return ErrCampaignNotFound
	}

	// Skip if no targeting configured
	if len(camp.TargetCountries) == 0 {
		return nil
	}

	country, err := f.geo.GetCountry(evt.IP)
	if err != nil {
		slog.Warn("geo lookup failed", "ip", evt.IP, "error", err)
		// We allow on lookup failure by default, or could block. 
		// For high RPS, we might want to allow to prevent complete ingestion halt.
		return nil 
	}

	for _, allowed := range camp.TargetCountries {
		if allowed == country {
			return nil
		}
	}

	return ErrGeoBlocked
}

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
		return err
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
		return err
	}

	if !ok {
		return ErrDuplicateEvent
	}

	return nil
}
