package ads

import (
	"context"
	_ "embed"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/mykhailov-ua/ad-event-processor/internal/metrics"
	redis "github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
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

type UnifiedFilter struct {
	rdbs                  []redis.UniversalClient
	sharder               Sharder
	script                *redis.Script
	registry              domain.CampaignRegistry
	repo                  domain.CampaignRepository
	rateLimit             int
	rateLimitWindow       time.Duration
	dupTTL                time.Duration
	idempotencyTTL        time.Duration
	clickAmountMicro      int64
	impressionAmountMicro int64
	streamName            string
	maxStreamLen          int
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
	clickAmount decimal.Decimal,
	impressionAmount decimal.Decimal,
	streamName string,
	maxStreamLen int,
) *UnifiedFilter {
	return &UnifiedFilter{
		rdbs:                  rdbs,
		sharder:               sharder,
		script:                redis.NewScript(unifiedFilterLua),
		registry:              registry,
		repo:                  repo,
		rateLimit:             rateLimit,
		rateLimitWindow:       rateLimitWindow,
		dupTTL:                dupTTL,
		idempotencyTTL:        idempotencyTTL,
		clickAmountMicro:      DecimalToMicro(clickAmount),
		impressionAmountMicro: DecimalToMicro(impressionAmount),
		streamName:            streamName,
		maxStreamLen:          maxStreamLen,
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
		id, _ := uuid.NewV7()
		evt.ClickID = id.String()
	}

	campaignIDStr := campInfo.IDStr
	customerIDStr := campInfo.CustomerIDStr

	amount := f.clickAmountMicro
	if evt.Type == "impression" {
		amount = f.impressionAmountMicro
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

	now := time.Now().In(campInfo.Location)

	wDate.buf = wDate.buf[:0]
	wDate.buf = appendDate(wDate.buf, now)
	currentDate := unsafeString(wDate.buf)

	currentHour := now.Hour() + 1 // 1-24

	wDS.buf = wDS.buf[:0]
	wDS.buf = append(wDS.buf, "budget:daily_spent:campaign:"...)
	wDS.buf = append(wDS.buf, campaignIDStr...)
	wDS.buf = append(wDS.buf, ':')
	wDS.buf = append(wDS.buf, currentDate...)
	dailySpendKey := unsafeString(wDS.buf)

	var fcapKey string
	wFcap.buf = wFcap.buf[:0]
	if evt.UserID != "" {
		wFcap.buf = append(wFcap.buf, "fcap:c:"...)
		wFcap.buf = append(wFcap.buf, campaignIDStr...)
		wFcap.buf = append(wFcap.buf, ":u:"...)
		wFcap.buf = append(wFcap.buf, evt.UserID...)
		fcapKey = unsafeString(wFcap.buf)
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

	isEven := 0
	if campInfo.PacingMode == domain.PacingModeEven {
		isEven = 1
	}

	// Loop executes up to 2 times to handle potential Redis budget cache misses.
	// If the unified Lua script returns -1, the budget key is loaded from the primary PostgreSQL
	// database and seeded into Redis, after which the script is re-run.
	for i := 0; i < 2; i++ {
		res, err := f.script.Run(ctx, rdb,
			keys,
			int(f.rateLimitWindow.Seconds()),
			f.rateLimit,
			int(f.dupTTL.Seconds()),
			amount,
			int(f.idempotencyTTL.Seconds()),
			campaignIDStr,
			customerIDStr,
			f.maxStreamLen,
			evt.ClickID,
			evt.Type,
			evt.Payload,
			evt.IP,
			evt.UA,
			isEven,
			campInfo.DailyBudgetMicro,
			currentHour,
			evt.UserID,
			campInfo.FreqLimit,
			campInfo.FreqWindow,
		).Int64()

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

			remaining := camp.BudgetLimit.Sub(camp.CurrentSpend)
			if remaining.IsNegative() {
				remaining = decimal.Zero
			}

			rdb.SetNX(ctx, budgetSourceKey, DecimalToMicro(remaining), 24*time.Hour)
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

