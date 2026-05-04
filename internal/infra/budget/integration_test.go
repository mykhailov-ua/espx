package budget_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/repository"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/mykhailov-ua/ad-event-processor/internal/infra/budget"
	infra_repo "github.com/mykhailov-ua/ad-event-processor/internal/infra/repository"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBudgetFlow_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()

	dbPool, err := pgxpool.New(ctx, "postgres://adpulse_user:secure_pass_123@localhost:5430/adpulse?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer dbPool.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6375",
		Password: "redis_secure_pass_456",
	})
	defer rdb.Close()

	queries := repository.New(dbPool)
	campaignRepo := infra_repo.NewCampaignRepo(queries)
	customerRepo := infra_repo.NewCustomerRepo(queries)
	registry := ads.NewRegistry(queries)

	budgetManager := budget.NewRedisBudgetManager(rdb, campaignRepo, 10*time.Second)
	syncWorker := budget.NewSyncWorker(rdb, campaignRepo, customerRepo, 100*time.Millisecond)

	customerID := uuid.New()
	campaignID := uuid.New()

	_, err = dbPool.Exec(ctx, "INSERT INTO customers (id, name, balance) VALUES ($1, $2, $3)", customerID, "Test Customer", 100.0)
	require.NoError(t, err)

	_, err = dbPool.Exec(ctx, "INSERT INTO campaigns (id, name, budget_limit, status, customer_id) VALUES ($1, $2, $3, $4, $5)",
		campaignID, "Test Campaign", 50.0, "ACTIVE", customerID)
	require.NoError(t, err)

	_, err = registry.Sync(ctx)
	require.NoError(t, err)

	err = rdb.Set(ctx, "budget:campaign:"+campaignID.String(), 50.0, 0).Err()
	require.NoError(t, err)

	filter := ads.NewBudgetFilter(budgetManager, registry)
	evt := &domain.Event{
		ClickID:    uuid.NewString(),
		CampaignID: campaignID,
		Type:       "click",
	}

	err = filter.Check(ctx, evt)
	require.NoError(t, err)

	val, err := rdb.Get(ctx, "budget:campaign:"+campaignID.String()).Float64()
	require.NoError(t, err)
	assert.Equal(t, 49.9, val)

	err = filter.Check(ctx, evt)
	require.NoError(t, err)
	val2, err := rdb.Get(ctx, "budget:campaign:"+campaignID.String()).Float64()
	require.NoError(t, err)
	assert.Equal(t, 49.9, val2)

	syncWorker.SyncAll(ctx)

	campaign, err := campaignRepo.GetByID(ctx, campaignID)
	require.NoError(t, err, "failed to get campaign from DB")
	require.NotNil(t, campaign)
	assert.Equal(t, 0.1, campaign.CurrentSpend)

	customer, err := customerRepo.GetByID(ctx, customerID)
	require.NoError(t, err, "failed to get customer from DB")
	require.NotNil(t, customer)
	assert.Equal(t, 99.9, customer.Balance)
}
