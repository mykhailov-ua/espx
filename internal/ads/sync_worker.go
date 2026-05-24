package ads

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/redis/go-redis/v9"
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
    return {"0", ""}
end

local txID = redis.call("GET", KEYS[4])
if not txID or txID == "" then
    txID = ARGV[2]
    redis.call("SET", KEYS[4], txID)
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
    redis.call("DEL", KEYS[4])
    return {"0", ""}
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
return {tostring(total), txID}
`

const commitSyncScript = `
local remaining = redis.call("INCRBY", KEYS[1], -tonumber(ARGV[1]))
if tonumber(remaining) <= 0 then
    redis.call("DEL", KEYS[1])
end

local sync_val = redis.call("GET", KEYS[5])
local inflight_val = redis.call("GET", KEYS[1])

local sync_num = 0
if sync_val then sync_num = tonumber(sync_val) end

local inflight_num = 0
if inflight_val then inflight_num = tonumber(inflight_val) end

if sync_num <= 0 and inflight_num <= 0 then
    redis.call("SREM", KEYS[2], ARGV[2])
end

redis.call("DEL", KEYS[3])
redis.call("DEL", KEYS[4])
return remaining
`

func (w *SyncWorker) syncEntity(ctx context.Context, prefix string, idStr string, updateFn func(context.Context, uuid.UUID, int64, string) error) {
	id, err := uuid.Parse(idStr)
	if err != nil {
		return
	}

	syncKey := "budget:sync:" + prefix + ":" + idStr
	inFlightKey := "budget:inflight:" + prefix + ":" + idStr
	lockKey := "budget:lock:" + prefix + ":" + idStr
	txKey := "budget:txid:" + prefix + ":" + idStr
	dirtySet := "budget:dirty_" + prefix + "s"

	newTxID := uuid.New().String()

	res, err := w.rdb.Eval(ctx, prepareSyncScript, []string{syncKey, inFlightKey, lockKey, txKey}, 60, newTxID).Result()
	if err != nil {
		return
	}

	arr, ok := res.([]any)
	if !ok || len(arr) < 2 {
		return
	}

	amountVal, ok1 := arr[0].(string)
	txIDVal, ok2 := arr[1].(string)
	if !ok1 || !ok2 {
		return
	}

	if amountVal == "0" {
		w.rdb.SRem(ctx, dirtySet, idStr)
		return
	}

	amountMicro, err := strconv.ParseInt(amountVal, 10, 64)
	if err != nil || amountMicro <= 0 {
		return
	}

	// lockKey is intentionally NOT released on updateFn errors. It expires via its TTL to prevent
	// parallel sync goroutines from racing into the same DB write within the same lock window.
	if err := updateFn(ctx, id, amountMicro, txIDVal); err == nil {
		w.rdb.Eval(ctx, commitSyncScript, []string{inFlightKey, dirtySet, lockKey, txKey, syncKey}, amountMicro, idStr)
	}
}

const maxConcurrency = 32

func (w *SyncWorker) syncCampaigns(ctx context.Context) {
	var cursor uint64
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	for {
		keys, nextCursor, err := w.rdb.SScan(ctx, "budget:dirty_campaigns", cursor, "", 100).Result()
		if err != nil {
			break
		}

		for _, idStr := range keys {
			wg.Add(1)
			sem <- struct{}{}
			go func(id string) {
				defer wg.Done()
				defer func() { <-sem }()
				w.syncEntity(ctx, "campaign", id, w.campaignRepo.UpdateSpend)
			}(idStr)
		}

		if nextCursor == 0 {
			break
		}
		cursor = nextCursor
	}
	wg.Wait()
}

func (w *SyncWorker) syncCustomers(ctx context.Context) {
	var cursor uint64
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	for {
		keys, nextCursor, err := w.rdb.SScan(ctx, "budget:dirty_customers", cursor, "", 100).Result()
		if err != nil {
			break
		}

		for _, idStr := range keys {
			wg.Add(1)
			sem <- struct{}{}
			go func(id string) {
				defer wg.Done()
				defer func() { <-sem }()
				w.syncEntity(ctx, "customer", id, w.customerRepo.UpdateBalance)
			}(idStr)
		}

		if nextCursor == 0 {
			break
		}
		cursor = nextCursor
	}
	wg.Wait()
}
