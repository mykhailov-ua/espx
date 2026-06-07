// Package ads provides discrete filter types that implement the EventFilter interface
// and a FilterEngine combinator that chains them in order. Each filter receives a
// *domain.Event and returns one of the package-level sentinel errors on rejection or
// nil on pass. A non-nil error from any filter short-circuits the chain; the caller
// inspects the error type to select the appropriate Prometheus label and response code.
//
// Shared key-building utilities (appendUUID, unsafeString, bufPool) are defined here
// and reused by budget_store.go and unified_filter.go.
package ads

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"espx/internal/domain"
	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

// Sentinel errors returned by filter implementations. Callers must compare with
// errors.Is; the string form is used only in log messages, not in API responses.
var (
	ErrRateLimitExceeded      = errors.New("rate limit exceeded")
	ErrDuplicateEvent         = errors.New("duplicate event detected")
	ErrBudgetExhausted        = errors.New("budget exhausted")
	ErrCampaignNotFound       = errors.New("campaign not found in registry")
	ErrPacingExhausted        = errors.New("pacing exhausted")
	ErrFreqLimitExceeded      = errors.New("frequency limit exceeded")
	ErrGeoBlocked             = errors.New("geo-targeting blocked")
	ErrFraudDetected          = errors.New("fraud detected")
	ErrEmergencyBreakerActive = errors.New("service temporarily unavailable (emergency breaker active)")
	ErrBidFloorNotMet         = errors.New("bid floor not met")
)

type bufWrapper struct {
	buf []byte
}

var bufPool = sync.Pool{
	New: func() any {
		return &bufWrapper{
			buf: make([]byte, 0, 128),
		}
	},
}

const hexChars = "0123456789abcdef"

func appendUUID(dst []byte, u uuid.UUID) []byte {
	return append(dst,
		hexChars[u[0]>>4], hexChars[u[0]&0xf],
		hexChars[u[1]>>4], hexChars[u[1]&0xf],
		hexChars[u[2]>>4], hexChars[u[2]&0xf],
		hexChars[u[3]>>4], hexChars[u[3]&0xf],
		'-',
		hexChars[u[4]>>4], hexChars[u[4]&0xf],
		hexChars[u[5]>>4], hexChars[u[5]&0xf],
		'-',
		hexChars[u[6]>>4], hexChars[u[6]&0xf],
		hexChars[u[7]>>4], hexChars[u[7]&0xf],
		'-',
		hexChars[u[8]>>4], hexChars[u[8]&0xf],
		hexChars[u[9]>>4], hexChars[u[9]&0xf],
		'-',
		hexChars[u[10]>>4], hexChars[u[10]&0xf],
		hexChars[u[11]>>4], hexChars[u[11]&0xf],
		hexChars[u[12]>>4], hexChars[u[12]&0xf],
		hexChars[u[13]>>4], hexChars[u[13]&0xf],
		hexChars[u[14]>>4], hexChars[u[14]&0xf],
		hexChars[u[15]>>4], hexChars[u[15]&0xf],
	)
}

func unsafeString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

type TimestampVal struct {
	val int64
	buf [32]byte
}

func (t *TimestampVal) MarshalBinary() ([]byte, error) {
	return strconv.AppendInt(t.buf[:0], t.val, 10), nil
}

var timestampPool = sync.Pool{
	New: func() any {
		return &TimestampVal{}
	},
}

// FraudFilter checks for anonymous-IP traffic and time-to-click (TTC) velocity.
// For impression events it stores a Unix-ms timestamp in Redis under a per-user+campaign
// key with a 10-minute TTL. For click events it reads that timestamp and computes the
// delta; if delta < ttcMin the click is annotated with a fraud reason string in
// evt.FraudReason (not rejected - the event is still forwarded to the fraud stream
// for later analysis). Datacenter IP detection rejects the event immediately.
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
		w := bufPool.Get().(*bufWrapper)
		w.buf = w.buf[:0]
		w.buf = append(w.buf, "imp_ts:"...)
		w.buf = append(w.buf, evt.UserID...)
		w.buf = append(w.buf, ':')
		w.buf = appendUUID(w.buf, evt.CampaignID)
		key := unsafeString(w.buf)

		tVal := timestampPool.Get().(*TimestampVal)
		tVal.val = time.Now().UnixMilli()
		err := f.rdb.Set(ctx, key, tVal, 10*time.Minute).Err()
		timestampPool.Put(tVal)
		bufPool.Put(w)

		if err != nil {
			slog.Error("failed to store impression timestamp in redis", "error", err, "user_id", evt.UserID, "campaign_id", evt.CampaignID)
		}
		return nil
	}

	if evt.Type == "click" {
		w := bufPool.Get().(*bufWrapper)
		w.buf = w.buf[:0]
		w.buf = append(w.buf, "imp_ts:"...)
		w.buf = append(w.buf, evt.UserID...)
		w.buf = append(w.buf, ':')
		w.buf = appendUUID(w.buf, evt.CampaignID)
		key := unsafeString(w.buf)

		ts, err := f.rdb.Get(ctx, key).Int64()
		bufPool.Put(w)

		if err == nil {
			delta := time.Since(time.UnixMilli(ts))
			if delta < f.ttcMin {
				evt.FraudReason = "low_ttc:" + delta.String()
				return ErrFraudDetected
			}
		}
	}

	return nil
}

// GeoFilter rejects events whose source IP does not match the campaign's
// TargetCountries allow-list. If the campaign has no target countries configured,
// all IPs are permitted. Geo-lookup failures are treated as pass to avoid false
// positives from MaxMind database gaps (private or newly-allocated ranges).
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
		return nil
	}

	if _, allowed := camp.TargetCountries[country]; allowed {
		return nil
	}

	return ErrGeoBlocked
}

// BudgetFilter delegates to domain.BudgetManager.CheckAndSpend. The charge amount
// differs by event type: clicks consume clickAmount micro-units, impressions consume
// impressionAmount. The customer ID is resolved from the registry to route the
// deduction to the correct customer balance in the Lua script.
type BudgetFilter struct {
	manager          domain.BudgetManager
	registry         domain.CampaignRegistry
	clickAmount      int64
	impressionAmount int64
}

func NewBudgetFilter(manager domain.BudgetManager, registry domain.CampaignRegistry, clickAmount, impressionAmount int64) *BudgetFilter {
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

// EventFilter is the common interface implemented by all filter types. The single
// Check method must be idempotent and safe for concurrent use.
type EventFilter interface {
	Check(ctx context.Context, evt *domain.Event) error
}

// FilterEngine runs a sequence of EventFilter instances in registration order,
// returning the first non-nil error. It does not recover from panics inside filters.
type FilterEngine struct {
	filters []EventFilter
}

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

// IPRateLimiter enforces a per-IP sliding-window rate limit via a Redis Lua script.
// The window is expressed in milliseconds (PEXPIRE) to avoid rounding at sub-second
// granularities. limitAny and windowAny are pre-boxed interface values to prevent
// per-call interface boxing on the Eval args slice.
type IPRateLimiter struct {
	rdb       redis.Cmdable
	limit     int
	window    time.Duration
	limitAny  any
	windowAny any
	keysPool  sync.Pool
	argsPool  sync.Pool
}

func NewIPRateLimiter(rdb redis.Cmdable, limit int, window time.Duration) *IPRateLimiter {
	return &IPRateLimiter{
		rdb:       rdb,
		limit:     limit,
		window:    window,
		limitAny:  limit,
		windowAny: int64(window.Milliseconds()),
		keysPool: sync.Pool{
			New: func() any {
				s := make([]string, 1)
				return &s
			},
		},
		argsPool: sync.Pool{
			New: func() any {
				s := make([]any, 2)
				return &s
			},
		},
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

	w := bufPool.Get().(*bufWrapper)
	w.buf = w.buf[:0]
	w.buf = append(w.buf, "ratelimit:ip:"...)
	w.buf = append(w.buf, evt.IP...)
	key := unsafeString(w.buf)

	keysPtr := l.keysPool.Get().(*[]string)
	keys := *keysPtr
	keys[0] = key

	argsPtr := l.argsPool.Get().(*[]any)
	args := *argsPtr
	args[0] = l.windowAny
	args[1] = l.limitAny

	res, err := l.rdb.Eval(ctx, rateLimitScript, keys, args...).Result()
	bufPool.Put(w)
	l.keysPool.Put(keysPtr)
	l.argsPool.Put(argsPtr)

	if err != nil {
		return err
	}

	if res.(int64) == 1 {
		return ErrRateLimitExceeded
	}

	return nil
}

// DuplicateEventFilter uses Redis SetNX to detect and reject replayed click IDs.
// The TTL (duplicateTTL) should cover the maximum stream reprocessing lag to prevent
// double-billing on processor restarts.
type DuplicateEventFilter struct {
	rdb redis.Cmdable
	ttl time.Duration
}

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

	w := bufPool.Get().(*bufWrapper)
	w.buf = w.buf[:0]
	w.buf = append(w.buf, "dup:"...)
	w.buf = append(w.buf, evt.Type...)
	w.buf = append(w.buf, ':')
	w.buf = append(w.buf, evt.ClickID...)
	key := unsafeString(w.buf)

	ok, err := f.rdb.SetNX(ctx, key, "1", f.ttl).Result()
	bufPool.Put(w)

	if err != nil {
		return err
	}

	if !ok {
		return ErrDuplicateEvent
	}

	return nil
}

// EmergencyBreakerFilter reads the EmergencyBreaker flag from the SettingsWatcher
// snapshot. When true, all events are rejected with ErrEmergencyBreakerActive without
// contacting Redis or any other downstream. The flag is set via the UPDATE_SETTINGS
// outbox event and propagated atomically through the SettingsWatcher snapshot swap.
type EmergencyBreakerFilter struct {
	watcher *SettingsWatcher
}

func NewEmergencyBreakerFilter(watcher *SettingsWatcher) *EmergencyBreakerFilter {
	return &EmergencyBreakerFilter{watcher: watcher}
}

func (f *EmergencyBreakerFilter) Check(ctx context.Context, evt *domain.Event) error {
	if f.watcher != nil && f.watcher.Get().EmergencyBreaker {
		return ErrEmergencyBreakerActive
	}
	return nil
}
