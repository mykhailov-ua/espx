package ads

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/google/uuid"

	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/mykhailov-ua/ad-event-processor/internal/metrics"
	redis "github.com/redis/go-redis/v9"
)

//go:embed unified_filter.lua
var unifiedFilterLua string

// keysPool recycles string slices to avoid array-to-slice heap allocations
// when passing keys to go-redis Eval/Run commands.
var keysPool = sync.Pool{
	New: func() any {
		s := make([]string, 11)
		return &s
	},
}

// argsPool recycles any slices to avoid variadic parameter heap allocation.
var argsPool = sync.Pool{
	New: func() any {
		s := make([]any, 19)
		return &s
	},
}

type StringVal struct {
	s string
}

func (sv *StringVal) MarshalBinary() ([]byte, error) {
	if len(sv.s) == 0 {
		return nil, nil
	}
	return unsafe.Slice(unsafe.StringData(sv.s), len(sv.s)), nil
}

type UnifiedStringWrappers struct {
	clickID StringVal
	evtType StringVal
	payload StringVal
	ip      StringVal
	ua      StringVal
	userID  StringVal
}

var unifiedWrappersPool = sync.Pool{
	New: func() any {
		return &UnifiedStringWrappers{}
	},
}

// appendDate formats a time.Time struct into a YYYYMMDD byte layout without allocating memory.
// It uses integer math to avoid formatting overhead from time.Format or fmt.Sprintf.
func appendDate(dst []byte, t time.Time) []byte {
	year, month, day := t.Date()
	return append(dst,
		byte('0'+year/1000),
		byte('0'+(year/100)%10),
		byte('0'+(year/10)%10),
		byte('0'+year%10),
		byte('0'+int(month)/10),
		byte('0'+int(month)%10),
		byte('0'+day/10),
		byte('0'+day%10),
	)
}



var (
	zeroAny any = 0
	oneAny  any = 1
)

var hourAnyCache [25]any

func init() {
	for i := 0; i <= 24; i++ {
		hourAnyCache[i] = i
	}
}

type UnifiedFilter struct {
	rdbs                     []redis.UniversalClient
	sharder                  Sharder
	script                   *redis.Script
	registry                 domain.CampaignRegistry
	repo                     domain.CampaignRepository
	geo                      GeoProvider
	geoFloors                sync.Map
	rateLimit                int
	rateLimitWindow          time.Duration
	dupTTL                   time.Duration
	idempotencyTTL           time.Duration
	clickAmountMicro         int64
	impressionAmountMicro    int64
	streamName               string
	maxStreamLen             int
	rateLimitWindowAny       any
	rateLimitAny             any
	dupTTLAny                any
	idempotencyTTLAny        any
	maxStreamLenAny          any
	clickAmountMicroAny      any
	impressionAmountMicroAny any
}

// SetGeoProvider configures the GeoIP resolution service.
func (f *UnifiedFilter) SetGeoProvider(geo GeoProvider) {
	// A setter maintains compatibility with existing constructors.
	f.geo = geo
}

// SetGeoBidFloor registers or updates a publisher floor limit for a specific geo.
func (f *UnifiedFilter) SetGeoBidFloor(country string, floor int64) {
	// Use sync.Map to guarantee thread-safe read-heavy configuration updates.
	f.geoFloors.Store(country, floor)
}

// parseBidMicro extracts the bid_micro value from a JSON byte slice without heap allocation.
// This is a highly optimized scanner that avoids reflection and unmarshaling overhead on the hot path.
func parseBidMicro(payload []byte) int64 {
	const key = `"bid_micro"`
	n := len(payload)
	kLen := len(key)
	if n < kLen {
		return 0
	}
	
	// Scan the payload for the raw key occurrence
	for i := 0; i <= n-kLen; i++ {
		if payload[i] == '"' && string(payload[i:i+kLen]) == key {
			// Find the colon separating the key and value
			idx := i + kLen
			for idx < n && (payload[idx] == ' ' || payload[idx] == '\t' || payload[idx] == ':') {
				if payload[idx] == ':' {
					idx++
					break
				}
				idx++
			}
			
			// Skip any whitespace before the number
			for idx < n && (payload[idx] == ' ' || payload[idx] == '\t') {
				idx++
			}
			
			// Parse the raw integer value directly
			var val int64
			hasDigit := false
			for idx < n && payload[idx] >= '0' && payload[idx] <= '9' {
				val = val*10 + int64(payload[idx]-'0')
				idx++
				hasDigit = true
			}
			if hasDigit {
				return val
			}
			return 0
		}
	}
	return 0
}

func NewUnifiedFilter(
	rdbs []redis.UniversalClient,
	sharder Sharder,
	registry domain.CampaignRegistry,
	repo domain.CampaignRepository,
	rateLimit int,
	rateLimitWindow time.Duration,
	dupTTL time.Duration,
	idempotencyTTL time.Duration,
	clickAmount int64,
	impressionAmount int64,
	streamName string,
	maxStreamLen int,
) *UnifiedFilter {
	return &UnifiedFilter{
		rdbs:                     rdbs,
		sharder:                  sharder,
		script:                   redis.NewScript(unifiedFilterLua),
		registry:                 registry,
		repo:                     repo,
		rateLimit:                rateLimit,
		rateLimitWindow:          rateLimitWindow,
		dupTTL:                   dupTTL,
		idempotencyTTL:           idempotencyTTL,
		clickAmountMicro:         clickAmount,
		impressionAmountMicro:    impressionAmount,
		streamName:               streamName,
		maxStreamLen:             maxStreamLen,
		rateLimitWindowAny:       int(rateLimitWindow.Seconds()),
		rateLimitAny:             rateLimit,
		dupTTLAny:                int(dupTTL.Seconds()),
		idempotencyTTLAny:        int(idempotencyTTL.Seconds()),
		maxStreamLenAny:          maxStreamLen,
		clickAmountMicroAny:      clickAmount,
		impressionAmountMicroAny: impressionAmount,
	}
}

func (f *UnifiedFilter) getRDB(campaignID uuid.UUID) redis.UniversalClient {
	if len(f.rdbs) <= 1 {
		return f.rdbs[0]
	}
	idx := f.sharder.GetShard(campaignID)
	return f.rdbs[idx%len(f.rdbs)]
}

func (f *UnifiedFilter) Check(ctx context.Context, evt *domain.Event) error {
	campInfo, ok := f.registry.GetCampaign(evt.CampaignID)
	if !ok {
		return ErrCampaignNotFound
	}

	if evt.ClickID == "" {
		id, err := uuid.NewV7()
		if err != nil {
			return fmt.Errorf("failed to generate click id: %w", err)
		}
		evt.ClickID = id.String()
	}

	// Verify dynamic geo bid floor if a geo provider and target floor are configured.
	// This filters out events that do not meet the minimum bid requirement for their resolved country code.
	if f.geo != nil {
		country, err := f.geo.GetCountry(evt.IP)
		if err == nil && country != "" {
			if floorVal, found := f.geoFloors.Load(country); found {
				floor := floorVal.(int64)
				if floor > 0 {
					bid := parseBidMicro(evt.Payload)
					if bid < floor {
						return ErrBidFloorNotMet
					}
				}
			}
		}
	}

	campaignIDStr := campInfo.IDStr
	customerIDStr := campInfo.CustomerIDStr

	amount := f.clickAmountMicroAny
	if evt.Type == "impression" {
		amount = f.impressionAmountMicroAny
	}

	rdb := f.getRDB(evt.CampaignID)

	wRL := bufPool.Get().(*bufWrapper)
	wDup := bufPool.Get().(*bufWrapper)
	wBgt := bufPool.Get().(*bufWrapper)
	wIdem := bufPool.Get().(*bufWrapper)
	wCSync := bufPool.Get().(*bufWrapper)
	wCustSync := bufPool.Get().(*bufWrapper)
	wDate := bufPool.Get().(*bufWrapper)
	wDS := bufPool.Get().(*bufWrapper)
	wFcap := bufPool.Get().(*bufWrapper)
	keysPtr := keysPool.Get().(*[]string)
	argsPtr := argsPool.Get().(*[]any)
	wrappers := unifiedWrappersPool.Get().(*UnifiedStringWrappers)

	defer func() {
		bufPool.Put(wRL)
		bufPool.Put(wDup)
		bufPool.Put(wBgt)
		bufPool.Put(wIdem)
		bufPool.Put(wCSync)
		bufPool.Put(wCustSync)
		bufPool.Put(wDate)
		bufPool.Put(wDS)
		bufPool.Put(wFcap)
		keysPool.Put(keysPtr)
		argsPool.Put(argsPtr)
		unifiedWrappersPool.Put(wrappers)
	}()

	wRL.buf = wRL.buf[:0]
	wRL.buf = append(wRL.buf, "rl:ip:"...)
	wRL.buf = append(wRL.buf, evt.IP...)
	rlKey := unsafeString(wRL.buf)

	wDup.buf = wDup.buf[:0]
	wDup.buf = append(wDup.buf, "dup:"...)
	wDup.buf = append(wDup.buf, evt.Type...)
	wDup.buf = append(wDup.buf, ':')
	wDup.buf = append(wDup.buf, evt.ClickID...)
	dupKey := unsafeString(wDup.buf)

	wBgt.buf = wBgt.buf[:0]
	wBgt.buf = append(wBgt.buf, "budget:campaign:"...)
	wBgt.buf = append(wBgt.buf, campaignIDStr...)
	budgetSourceKey := unsafeString(wBgt.buf)

	wIdem.buf = wIdem.buf[:0]
	wIdem.buf = append(wIdem.buf, "idempotency:click:"...)
	wIdem.buf = append(wIdem.buf, evt.ClickID...)
	idempotencyKey := unsafeString(wIdem.buf)

	wCSync.buf = wCSync.buf[:0]
	wCSync.buf = append(wCSync.buf, "budget:sync:campaign:"...)
	wCSync.buf = append(wCSync.buf, campaignIDStr...)
	campaignSyncKey := unsafeString(wCSync.buf)

	wCustSync.buf = wCustSync.buf[:0]
	wCustSync.buf = append(wCustSync.buf, "budget:sync:customer:"...)
	wCustSync.buf = append(wCustSync.buf, customerIDStr...)
	customerSyncKey := unsafeString(wCustSync.buf)

	dirtyCampaignsKey := "budget:dirty_campaigns"
	dirtyCustomersKey := "budget:dirty_customers"
	streamKey := f.streamName

	var now time.Time
	if campInfo.Location == time.UTC {
		now = time.Now().UTC()
	} else {
		now = time.Now().In(campInfo.Location)
	}

	wDate.buf = wDate.buf[:0]
	wDate.buf = appendDate(wDate.buf, now)
	currentDate := unsafeString(wDate.buf)

	wDS.buf = wDS.buf[:0]
	wDS.buf = append(wDS.buf, "budget:daily_spent:campaign:"...)
	wDS.buf = append(wDS.buf, campaignIDStr...)
	wDS.buf = append(wDS.buf, ':')
	wDS.buf = append(wDS.buf, currentDate...)
	dailySpendKey := unsafeString(wDS.buf)

	var fcapKey string
	wFcap.buf = wFcap.buf[:0]
	if evt.UserID != "" {
		if campInfo.BrandFcapKey != "" {
			wFcap.buf = append(wFcap.buf, campInfo.BrandFcapKey...)
			wFcap.buf = append(wFcap.buf, ":u:"...)
			wFcap.buf = append(wFcap.buf, evt.UserID...)
			fcapKey = unsafeString(wFcap.buf)
		} else {
			wFcap.buf = append(wFcap.buf, "fcap:c:"...)
			wFcap.buf = append(wFcap.buf, campaignIDStr...)
			wFcap.buf = append(wFcap.buf, ":u:"...)
			wFcap.buf = append(wFcap.buf, evt.UserID...)
			fcapKey = unsafeString(wFcap.buf)
		}
	} else {
		fcapKey = "fcap:ignored"
	}

	keys := *keysPtr
	keys[0] = rlKey
	keys[1] = dupKey
	keys[2] = budgetSourceKey
	keys[3] = idempotencyKey
	keys[4] = campaignSyncKey
	keys[5] = customerSyncKey
	keys[6] = dirtyCampaignsKey
	keys[7] = dirtyCustomersKey
	keys[8] = streamKey
	keys[9] = dailySpendKey
	keys[10] = fcapKey

	isEven := zeroAny
	if campInfo.PacingMode == domain.PacingModeEven {
		isEven = oneAny
	}

	hr := now.Hour() + 1
	if hr < 1 {
		hr = 1
	} else if hr > 24 {
		hr = 24
	}
	currentHour := hourAnyCache[hr]

	wrappers.clickID.s = evt.ClickID
	wrappers.evtType.s = evt.Type
	wrappers.payload.s = unsafeString(evt.Payload)
	wrappers.ip.s = evt.IP
	wrappers.ua.s = evt.UA
	wrappers.userID.s = evt.UserID

	args := *argsPtr
	args[0] = f.rateLimitWindowAny
	args[1] = f.rateLimitAny
	args[2] = f.dupTTLAny
	args[3] = amount
	args[4] = f.idempotencyTTLAny
	args[5] = campInfo.IDStrAny
	args[6] = campInfo.CustomerIDStrAny
	args[7] = f.maxStreamLenAny
	args[8] = &wrappers.clickID
	args[9] = &wrappers.evtType
	args[10] = &wrappers.payload
	args[11] = &wrappers.ip
	args[12] = &wrappers.ua
	args[13] = isEven
	args[14] = campInfo.DailyBudgetMicroAny
	args[15] = currentHour
	args[16] = &wrappers.userID
	args[17] = campInfo.FreqLimitAny
	args[18] = campInfo.FreqWindowAny

	// Loop executes up to 2 times to handle potential Redis budget cache misses.
	// If the unified Lua script returns -1, the budget key is loaded from the primary PostgreSQL
	// database and seeded into Redis, after which the script is re-run.
	for i := 0; i < 2; i++ {
		cmd := f.script.EvalSha(ctx, rdb, keys, args...)
		if err := cmd.Err(); err != nil && (errors.Is(err, redis.ErrNoScript) || err.Error() == "NOSCRIPT No matching script. Please use EVAL.") {
			cmd = f.script.Eval(ctx, rdb, keys, args...)
		}
		res, err := cmd.Int64()

		if err != nil {
			return err
		}

		if res == -1 {
			if i > 0 {
				return fmt.Errorf("budget cache miss on retry: %w", ErrBudgetExhausted)
			}

			camp, err := f.repo.GetByID(ctx, evt.CampaignID)
			if err != nil {
				return fmt.Errorf("failed to load campaign from db: %w", err)
			}

			remaining := camp.BudgetLimit - camp.CurrentSpend
			if remaining < 0 {
				remaining = 0
			}

			rdb.SetNX(ctx, budgetSourceKey, remaining, 24*time.Hour)
			continue
		}

		switch res {
		case 1:
			return ErrRateLimitExceeded
		case 2:
			return ErrDuplicateEvent
		case 3:
			return ErrBudgetExhausted
		case 4:
			return ErrPacingExhausted
		case 5:
			return ErrFreqLimitExceeded
		default:
			metrics.EventsProcessed.Inc()
			return nil
		}
	}

	return nil
}

