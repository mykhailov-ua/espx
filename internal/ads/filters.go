package ads

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	redis "github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

var (
	ErrRateLimitExceeded     = errors.New("rate limit exceeded")
	ErrDuplicateEvent        = errors.New("duplicate event detected")
	ErrBudgetExhausted       = errors.New("budget exhausted")
	ErrCampaignNotFound      = errors.New("campaign not found in registry")
	ErrPacingExhausted       = errors.New("pacing exhausted")
	ErrFreqLimitExceeded     = errors.New("frequency limit exceeded")
	ErrGeoBlocked            = errors.New("geo-targeting blocked")
	ErrFraudDetected         = errors.New("fraud detected")
	ErrEmergencyBreakerActive = errors.New("service temporarily unavailable (emergency breaker active)")
)


type bufWrapper struct {
	buf []byte
}

// bufPool recycles byte buffer wrappers to avoid heap allocation overhead during string key formatting.
var bufPool = sync.Pool{
	New: func() any {
		return &bufWrapper{
			buf: make([]byte, 0, 128),
		}
	},
}

const hexChars = "0123456789abcdef"

// appendUUID performs zero-allocation hexadecimal formatting of a 16-byte UUID directly into a destination slice.
// This bypasses the standard google/uuid.String() method, which forces heap allocations.
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

// unsafeString performs zero-copy conversion from a byte slice to a string to eliminate heap allocation.
// The caller must guarantee that the underlying byte slice is not mutated while the returned string is referenced.
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
		return nil
	}

	if _, allowed := camp.TargetCountries[country]; allowed {
		return nil
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

// EmergencyBreakerFilter checks the in-memory breaker status using zero-allocation atomic values from SettingsWatcher,
// providing an instant failsafe to drop ingestion traffic without stressing the DB/Redis pool.
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

