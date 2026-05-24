package management

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
	"github.com/mykhailov-ua/ad-event-processor/internal/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagementQueries(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanup := database.SetupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	queries := db.New(pool)

	customerID := uuid.New()
	cust, err := queries.CreateCustomer(ctx, db.CreateCustomerParams{
		ID:       pgtype.UUID{Bytes: customerID, Valid: true},
		Name:     "Test db.Customer",
		Balance:  1_000_000_000,
		Currency: "USD",
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1_000_000_000), cust.Balance)

	cust, err = queries.UpdateCustomerBalanceManagement(ctx, db.UpdateCustomerBalanceManagementParams{
		ID:      pgtype.UUID{Bytes: customerID, Valid: true},
		Balance: 500_000_000,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1_500_000_000), cust.Balance)

	campaignID := uuid.New()
	camp, err := queries.CreateCampaign(ctx, db.CreateCampaignParams{
		ID:          pgtype.UUID{Bytes: campaignID, Valid: true},
		Name:        "Management Test Campaign",
		BudgetLimit: 100_000_000,
		Status:      db.CampaignStatusTypeACTIVE,
		CustomerID:  pgtype.UUID{Bytes: customerID, Valid: true},
		PacingMode:  db.PacingModeTypeASAP,
		DailyBudget: 0,
		Timezone:    "UTC",
	})
	require.NoError(t, err)
	assert.Equal(t, campaignID, uuid.UUID(camp.ID.Bytes))
}
