package tests

import (
	"context"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/internal/domain"
	"github.com/google/uuid"
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

	_, err := dbPool.Exec(ctx, "INSERT INTO customers (id, name, balance) VALUES ($1, $2, $3)", customerID, "Test Customer", 100_000_000)
	require.NoError(t, err)

	_, err = dbPool.Exec(ctx, "INSERT INTO campaigns (id, name, budget_limit, status, customer_id) VALUES ($1, $2, $3, $4, $5)",
		campaignID, "Test Campaign", 50_000_000, "ACTIVE", customerID)
	require.NoError(t, err)

	_, err = registry.Sync(ctx)
	require.NoError(t, err)

	err = rdb.Set(ctx, "budget:campaign:"+campaignID.String(), 50_000_000, 0).Err()
	require.NoError(t, err)

	filter := ads.NewBudgetFilter(budgetManager, registry, 100_000, 10_000)
	evt := &domain.Event{
		ClickID:    uuid.NewString(),
		CampaignID: campaignID,
		Type:       "click",
	}

	err = filter.Check(ctx, evt)
	require.NoError(t, err)

	val, err := rdb.Get(ctx, "budget:campaign:"+campaignID.String()).Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(49_900_000), val)

	err = filter.Check(ctx, evt)
	require.NoError(t, err)
	val2, err := rdb.Get(ctx, "budget:campaign:"+campaignID.String()).Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(49_900_000), val2)

	syncWorker.SyncAll(ctx)

	campaign, err := campaignRepo.GetByID(ctx, campaignID)
	require.NoError(t, err, "failed to get campaign from DB")
	require.NotNil(t, campaign)
	assert.Equal(t, int64(100_000), campaign.CurrentSpend)

	customer, err := customerRepo.GetByID(ctx, customerID)
	require.NoError(t, err, "failed to get customer from DB")
	require.NotNil(t, customer)
	assert.Equal(t, int64(99_900_000), customer.Balance)
}
