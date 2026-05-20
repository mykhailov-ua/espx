package ads

import (
// "github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
)

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

type SyncWorker struct {
	rdb          redis.Cmdable
	campaignRepo domain.CampaignRepository
	customerRepo domain.CustomerRepository
	interval     time.Duration
	wg           sync.WaitGroup
}

func NewSyncWorker(
	rdb redis.Cmdable,
	campaignRepo domain.CampaignRepository,
	customerRepo domain.CustomerRepository,
	interval time.Duration,
) *SyncWorker {
	return &SyncWorker{
		rdb:          rdb,
		campaignRepo: campaignRepo,
		customerRepo: customerRepo,
		interval:     interval,
	}
}

func (w *SyncWorker) Start(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				w.SyncAll(context.Background())
				return
			case <-ticker.C:
				w.SyncAll(ctx)
			}
		}
	}()
}

func (w *SyncWorker) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *SyncWorker) SyncAll(ctx context.Context) {
	w.syncCampaigns(ctx)
	w.syncCustomers(ctx)
}

const prepareSyncScript = `
if redis.call("EXISTS", KEYS[3]) == 1 then
    return "0"
end

local inflight = redis.call("GET", KEYS[2])
local current = redis.call("GET", KEYS[1])

local total = 0
if inflight then total = total + tonumber(inflight) end
if current then total = total + tonumber(current) end

if total <= 0 then
    if current and tonumber(current) <= 0 then
        redis.call("DEL", KEYS[1])
    end
    return "0"
end

if current and tonumber(current) > 0 then
    local remaining = redis.call("INCRBY", KEYS[1], -tonumber(current))
    redis.call("INCRBY", KEYS[2], tonumber(current))
    if tonumber(remaining) <= 0 then
        redis.call("DEL", KEYS[1])
    end
elseif current and tonumber(current) <= 0 then
    redis.call("DEL", KEYS[1])
end

redis.call("SET", KEYS[3], "1", "EX", ARGV[1])
return tostring(total)
`

const commitSyncScript = `
local remaining = redis.call("INCRBY", KEYS[1], -tonumber(ARGV[1]))
if tonumber(remaining) <= 0 then
    redis.call("DEL", KEYS[1])
    redis.call("SREM", KEYS[2], ARGV[2])
end
redis.call("DEL", KEYS[3])
return remaining
`

func (w *SyncWorker) syncEntity(ctx context.Context, prefix string, idStr string, updateFn func(context.Context, uuid.UUID, decimal.Decimal) error) {
	id, err := uuid.Parse(idStr)
	if err != nil {
		return
	}

	syncKey := "budget:sync:" + prefix + ":" + idStr
	inFlightKey := "budget:inflight:" + prefix + ":" + idStr
	lockKey := "budget:lock:" + prefix + ":" + idStr
	dirtySet := "budget:dirty_" + prefix + "s"

	amountStr, err := w.rdb.Eval(ctx, prepareSyncScript, []string{syncKey, inFlightKey, lockKey}, 60).Result()
	if err != nil || amountStr == "0" {
		if amountStr == "0" {
			w.rdb.SRem(ctx, dirtySet, idStr)
		}
		return
	}

	amountMicro, err := strconv.ParseInt(amountStr.(string), 10, 64)
	if err != nil || amountMicro <= 0 {
		return
	}

	amount := MicroToDecimal(amountMicro)

	if err := updateFn(ctx, id, amount); err == nil {
		w.rdb.Eval(ctx, commitSyncScript, []string{inFlightKey, dirtySet, lockKey}, amountMicro, idStr)
	} else {
		w.rdb.Del(ctx, lockKey)
	}
}

func (w *SyncWorker) syncCampaigns(ctx context.Context) {
	var cursor uint64
	for {
		keys, nextCursor, err := w.rdb.SScan(ctx, "budget:dirty_campaigns", cursor, "", 100).Result()
		if err != nil {
			return
		}

		for _, idStr := range keys {
			w.syncEntity(ctx, "campaign", idStr, w.campaignRepo.UpdateSpend)
		}

		if nextCursor == 0 {
			break
		}
		cursor = nextCursor
	}
}

func (w *SyncWorker) syncCustomers(ctx context.Context) {
	var cursor uint64
	for {
		keys, nextCursor, err := w.rdb.SScan(ctx, "budget:dirty_customers", cursor, "", 100).Result()
		if err != nil {
			return
		}

		for _, idStr := range keys {
			w.syncEntity(ctx, "customer", idStr, w.customerRepo.UpdateBalance)
		}

		if nextCursor == 0 {
			break
		}
		cursor = nextCursor
	}
}
