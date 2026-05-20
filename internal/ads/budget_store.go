package ads

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

// keys2Pool recycles string slices to avoid array-to-slice heap allocations
// when passing keys to go-redis Eval/Run commands.
var keys2Pool = sync.Pool{
	New: func() any {
		s := make([]string, 2)
		return &s
	},
}

const budgetLuaScript = `
if redis.call("EXISTS", KEYS[2]) == 1 then
    return 1
end

local b = redis.call("GET", KEYS[1])
if not b then
    return -1
end

local budget = tonumber(b)
local amount = tonumber(ARGV[1])

if budget < amount then
    return 0
end

redis.call("INCRBY", KEYS[1], -amount)
redis.call("INCRBY", "budget:sync:campaign:" .. ARGV[3], ARGV[1])
redis.call("INCRBY", "budget:sync:customer:" .. ARGV[4], ARGV[1])
redis.call("SADD", "budget:dirty_campaigns", ARGV[3])
redis.call("SADD", "budget:dirty_customers", ARGV[4])
redis.call("SET", KEYS[2], "1", "EX", ARGV[2])

return 1
`

type RedisBudgetManager struct {
	rdb            redis.Cmdable
	campaignRepo   domain.CampaignRepository
	idempotencyTTL time.Duration
}

func NewRedisBudgetManager(rdb redis.Cmdable, repo domain.CampaignRepository, idempotencyTTL time.Duration) *RedisBudgetManager {
	return &RedisBudgetManager{
		rdb:            rdb,
		campaignRepo:   repo,
		idempotencyTTL: idempotencyTTL,
	}
}

func (m *RedisBudgetManager) CheckAndSpend(ctx context.Context, customerID, campaignID uuid.UUID, clickID string, amount decimal.Decimal) (bool, error) {
	wCampKey := bufPool.Get().(*bufWrapper)
	wIdemKey := bufPool.Get().(*bufWrapper)
	wCampID := bufPool.Get().(*bufWrapper)
	wCustID := bufPool.Get().(*bufWrapper)
	keysPtr := keys2Pool.Get().(*[]string)

	defer func() {
		bufPool.Put(wCampKey)
		bufPool.Put(wIdemKey)
		bufPool.Put(wCampID)
		bufPool.Put(wCustID)
		keys2Pool.Put(keysPtr)
	}()

	wCampKey.buf = wCampKey.buf[:0]
	wCampKey.buf = append(wCampKey.buf, "budget:campaign:"...)
	wCampKey.buf = appendUUID(wCampKey.buf, campaignID)
	campaignKey := unsafeString(wCampKey.buf)

	wIdemKey.buf = wIdemKey.buf[:0]
	wIdemKey.buf = append(wIdemKey.buf, "idempotency:click:"...)
	wIdemKey.buf = append(wIdemKey.buf, clickID...)
	idempotencyKey := unsafeString(wIdemKey.buf)

	wCampID.buf = wCampID.buf[:0]
	wCampID.buf = appendUUID(wCampID.buf, campaignID)
	campaignIDStr := unsafeString(wCampID.buf)

	wCustID.buf = wCustID.buf[:0]
	wCustID.buf = appendUUID(wCustID.buf, customerID)
	customerIDStr := unsafeString(wCustID.buf)

	keys := *keysPtr
	keys[0] = campaignKey
	keys[1] = idempotencyKey

	amountMicro := DecimalToMicro(amount)

	res, err := m.rdb.Eval(ctx, budgetLuaScript, keys,
		amountMicro,
		int(m.idempotencyTTL.Seconds()),
		campaignIDStr,
		customerIDStr,
	).Int64()
	if err != nil {
		return false, err
	}

	// If res is -1, the campaign budget key does not exist in Redis.
	// We load the remaining budget from PostgreSQL, seed the budget in Redis, and retry the script execution.
	if res == -1 {
		camp, err := m.campaignRepo.GetByID(ctx, campaignID)
		if err != nil {
			return false, fmt.Errorf("failed to load campaign from db on cache miss: %w", err)
		}

		remaining := camp.BudgetLimit.Sub(camp.CurrentSpend)
		if remaining.IsNegative() {
			remaining = decimal.Zero
		}

		m.rdb.SetNX(ctx, campaignKey, DecimalToMicro(remaining), 24*time.Hour)

		return m.CheckAndSpend(ctx, customerID, campaignID, clickID, amount)
	}

	return res == 1, nil
}
