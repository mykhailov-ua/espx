package ads

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	redis "github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)



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
	isAnon, err := f.geo.IsAnonymous(evt.IP)
	if err == nil && isAnon {
		evt.FraudReason = "datacenter_ip"
		return ErrFraudDetected
	}

	if evt.Type == "impression" {
		key := "imp_ts:" + evt.UserID + ":" + evt.CampaignID.String()
		if err := f.rdb.Set(ctx, key, time.Now().UnixMilli(), 10*time.Minute).Err(); err != nil {
			slog.Error("failed to store impression timestamp in redis", "error", err, "user_id", evt.UserID, "campaign_id", evt.CampaignID)
		}
		return nil
	}

	if evt.Type == "click" {
		key := "imp_ts:" + evt.UserID + ":" + evt.CampaignID.String()
		ts, err := f.rdb.Get(ctx, key).Int64()
		if err == nil {
			delta := time.Since(time.UnixMilli(ts))
			if delta < f.ttcMin {
				evt.FraudReason = "low_ttc:" + delta.String()
			}
		}
	}

	return nil
}

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

	if len(camp.TargetCountries) == 0 {
		return nil
	}

	country, err := f.geo.GetCountry(evt.IP)
	if err != nil {
		slog.Warn("geo lookup failed", "ip", evt.IP, "error", err)
		// Fail-open strategy prevents geo-provider outages from halting ingestion.
		return nil
	}

	for _, allowed := range camp.TargetCountries {
		if allowed == country {
			return nil
		}
	}

	return ErrGeoBlocked
}

type BudgetFilter struct {
	manager          domain.BudgetManager
	registry         domain.CampaignRegistry
	clickAmount      decimal.Decimal
	impressionAmount decimal.Decimal
}

func NewBudgetFilter(manager domain.BudgetManager, registry domain.CampaignRegistry, clickAmount, impressionAmount decimal.Decimal) *BudgetFilter {
	return &BudgetFilter{
		manager:          manager,
		registry:         registry,
		clickAmount:      clickAmount,
		impressionAmount: impressionAmount,
	}
}

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

type EventFilter interface {
	Check(ctx context.Context, evt *domain.Event) error
}

type FilterEngine struct {
	filters []EventFilter
}

// NewFilterEngine constructs a pipeline from the provided filter set.
func NewFilterEngine(filters ...EventFilter) *FilterEngine {
	return &FilterEngine{filters: filters}
}

func (e *FilterEngine) Check(ctx context.Context, evt *domain.Event) error {
	for _, f := range e.filters {
		if err := f.Check(ctx, evt); err != nil {
			return err
		}
	}
	return nil
}

type IPRateLimiter struct {
	rdb    redis.Cmdable
	limit  int
	window time.Duration
}

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

func (l *IPRateLimiter) Check(ctx context.Context, evt *domain.Event) error {
	if evt.IP == "" {
		return nil
	}

	key := "ratelimit:ip:" + evt.IP
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

func (f *DuplicateEventFilter) Check(ctx context.Context, evt *domain.Event) error {
	if evt.ClickID == "" {
		return nil
	}

	key := "dup:" + evt.Type + ":" + evt.ClickID

	ok, err := f.rdb.SetNX(ctx, key, "1", f.ttl).Result()
	if err != nil {
		return err
	}

	if !ok {
		return ErrDuplicateEvent
	}

	return nil
}
