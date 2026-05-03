package budget

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
)

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

redis.call("INCRBYFLOAT", KEYS[1], -amount)
redis.call("INCRBYFLOAT", "budget:sync:campaign:" .. ARGV[3], ARGV[1])
redis.call("INCRBYFLOAT", "budget:sync:customer:" .. ARGV[4], ARGV[1])
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

func (m *RedisBudgetManager) CheckAndSpend(ctx context.Context, customerID, campaignID uuid.UUID, clickID string, amount float64) (bool, error) {
	campaignKey := fmt.Sprintf("budget:campaign:%s", campaignID)
	idempotencyKey := fmt.Sprintf("idempotency:click:%s", clickID)

	res, err := m.rdb.Eval(ctx, budgetLuaScript, []string{campaignKey, idempotencyKey},
		amount,
		int(m.idempotencyTTL.Seconds()),
		campaignID.String(),
		customerID.String(),
	).Int64()
	if err != nil {
		return false, err
	}

	if res == -1 {
		camp, err := m.campaignRepo.GetByID(ctx, campaignID)
		if err != nil {
			return false, fmt.Errorf("failed to load campaign from db on cache miss: %w", err)
		}
		
		remaining := camp.BudgetLimit - camp.CurrentSpend
		if remaining < 0 {
			remaining = 0
		}

		// Seed Redis with SetNX to prevent concurrent overwrite
		m.rdb.SetNX(ctx, campaignKey, remaining, 24*time.Hour)

		return m.CheckAndSpend(ctx, customerID, campaignID, clickID, amount)
	}

	return res == 1, nil
}
