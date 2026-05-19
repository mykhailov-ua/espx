package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBudgetFlow_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()

	dbPool, cleanupDB := setupTestDB(t)
	defer cleanupDB()

	rdb, cleanupRedis := setupTestRedis(t)
	defer cleanupRedis()

	queries := db.New(dbPool)
	campaignRepo := ads.NewCampaignRepo(queries)
	customerRepo := ads.NewCustomerRepo(queries)
	registry := ads.NewRegistry(queries)

	budgetManager := ads.NewRedisBudgetManager(rdb, campaignRepo, 10*time.Second)
	syncWorker := ads.NewSyncWorker(rdb, campaignRepo, customerRepo, 100*time.Millisecond)

	customerID := uuid.New()
	campaignID := uuid.New()

	_, err := dbPool.Exec(ctx, "INSERT INTO customers (id, name, balance) VALUES ($1, $2, $3)", customerID, "Test Customer", 100.0)
	require.NoError(t, err)

	_, err = dbPool.Exec(ctx, "INSERT INTO campaigns (id, name, budget_limit, status, customer_id) VALUES ($1, $2, $3, $4, $5)",
		campaignID, "Test Campaign", 50.0, "ACTIVE", customerID)
	require.NoError(t, err)

	_, err = registry.Sync(ctx)
	require.NoError(t, err)

	err = rdb.Set(ctx, "budget:campaign:"+campaignID.String(), 50.0, 0).Err()
	require.NoError(t, err)

	filter := ads.NewBudgetFilter(budgetManager, registry, decimal.NewFromFloat(0.10), decimal.NewFromFloat(0.01))
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
	assert.True(t, campaign.CurrentSpend.Equal(decimal.NewFromFloat(0.1)))

	customer, err := customerRepo.GetByID(ctx, customerID)
	require.NoError(t, err, "failed to get customer from DB")
	require.NotNil(t, customer)
	assert.True(t, customer.Balance.Equal(decimal.NewFromFloat(99.9)))
}
