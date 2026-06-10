package ads

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"espx/internal/domain"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type budgetArgs struct {
	campKeyBuf [64]byte
	idemKeyBuf [64]byte
	campIDBuf  [36]byte
	custIDBuf  [36]byte
	amountBuf  [32]byte
	ttlBuf     [32]byte

	campaignKey    string
	idempotencyKey string
	campaignIDStr  string
	customerIDStr  string
	amountStr      string
	ttlStr         string

	keys [2]string
	args [4]any
}

var budgetArgsPool = sync.Pool{
	New: func() any {
		return &budgetArgs{}
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

// unsafeString keys must not outlive Eval. Cache miss (-1) allows one PG reload.
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

func (m *RedisBudgetManager) CheckAndSpend(ctx context.Context, customerID, campaignID uuid.UUID, clickID string, amount int64) (bool, error) {
	ba := budgetArgsPool.Get().(*budgetArgs)
	defer budgetArgsPool.Put(ba)

	campKeySlice := ba.campKeyBuf[:0]
	campKeySlice = append(campKeySlice, "budget:campaign:"...)
	campKeySlice = appendUUID(campKeySlice, campaignID)
	ba.campaignKey = unsafeString(campKeySlice)

	idemKeySlice := ba.idemKeyBuf[:0]
	idemKeySlice = append(idemKeySlice, "idempotency:click:"...)
	idemKeySlice = append(idemKeySlice, clickID...)
	ba.idempotencyKey = unsafeString(idemKeySlice)

	campIDSlice := ba.campIDBuf[:0]
	campIDSlice = appendUUID(campIDSlice, campaignID)
	ba.campaignIDStr = unsafeString(campIDSlice)

	custIDSlice := ba.custIDBuf[:0]
	custIDSlice = appendUUID(custIDSlice, customerID)
	ba.customerIDStr = unsafeString(custIDSlice)

	amountSlice := ba.amountBuf[:0]
	amountSlice = strconv.AppendInt(amountSlice, amount, 10)
	ba.amountStr = unsafeString(amountSlice)

	ttlSlice := ba.ttlBuf[:0]
	ttlSlice = strconv.AppendInt(ttlSlice, int64(m.idempotencyTTL.Seconds()), 10)
	ba.ttlStr = unsafeString(ttlSlice)

	ba.keys[0] = ba.campaignKey
	ba.keys[1] = ba.idempotencyKey

	ba.args[0] = &ba.amountStr
	ba.args[1] = &ba.ttlStr
	ba.args[2] = &ba.campaignIDStr
	ba.args[3] = &ba.customerIDStr

	for i := 0; i < 2; i++ {
		res, err := m.rdb.Eval(ctx, budgetLuaScript, ba.keys[:], ba.args[:]...).Int64()
		if err != nil {
			return false, err
		}

		if res == -1 {
			if i > 0 {
				return false, fmt.Errorf("budget cache miss on retry")
			}

			camp, err := m.campaignRepo.GetByID(ctx, campaignID)
			if err != nil {
				return false, fmt.Errorf("failed to load campaign from db on cache miss: %w", err)
			}

			remaining := camp.BudgetLimit - camp.CurrentSpend
			if remaining < 0 {
				remaining = 0
			}

			m.rdb.SetNX(ctx, ba.campaignKey, remaining, 24*time.Hour)
			continue
		}

		return res == 1, nil
	}

	return false, fmt.Errorf("budget cache miss on retry")
}
