package integration

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/repository"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCampaignQueries(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	queries := repository.New(pool)

	campaignID := uuid.New()
	_, err := pool.Exec(ctx,
		"INSERT INTO campaigns (id, name, status) VALUES ($1, $2, $3)",
		campaignID, "Test Campaign", "ACTIVE")
	require.NoError(t, err)

	ids, err := queries.ListCampaignIDs(ctx)
	require.NoError(t, err)

	found := false
	for _, id := range ids {
		if id.Bytes == campaignID {
			found = true
			break
		}
	}
	assert.True(t, found, "Active campaign ID should be in the list")

	inactiveID := uuid.New()
	_, err = pool.Exec(ctx,
		"INSERT INTO campaigns (id, name, status) VALUES ($1, $2, $3)",
		inactiveID, "Inactive Campaign", "PAUSED")
	require.NoError(t, err)

	ids, err = queries.ListCampaignIDs(ctx)
	require.NoError(t, err)
	for _, id := range ids {
		assert.NotEqual(t, inactiveID, id.Bytes, "Inactive campaign should not be listed")
	}
}

func TestStatsBatching(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	queries := repository.New(pool)

	campaignID := uuid.New()
	_, err := pool.Exec(ctx, "INSERT INTO campaigns (id, name, status) VALUES ($1, $2, $3)", campaignID, "Stats Test", "ACTIVE")
	require.NoError(t, err)

	err = queries.UpdateCampaignStatsBatch(ctx, repository.UpdateCampaignStatsBatchParams{
		CampaignIds: []pgtype.UUID{{Bytes: campaignID, Valid: true}},
		Impressions: []int64{10},
		Clicks:      []int64{5},
		Conversions: []int64{1},
	})
	require.NoError(t, err)

	err = queries.UpdateCampaignStatsBatch(ctx, repository.UpdateCampaignStatsBatchParams{
		CampaignIds: []pgtype.UUID{{Bytes: campaignID, Valid: true}},
		Impressions: []int64{20},
		Clicks:      []int64{2},
		Conversions: []int64{0},
	})
	require.NoError(t, err)

	var imps, clicks, convs int64
	err = pool.QueryRow(ctx,
		"SELECT impressions_count, clicks_count, conversions_count FROM campaign_stats WHERE campaign_id = $1 AND date = CURRENT_DATE",
		campaignID).Scan(&imps, &clicks, &convs)

	require.NoError(t, err)
	assert.Equal(t, int64(30), imps)
	assert.Equal(t, int64(7), clicks)
	assert.Equal(t, int64(1), convs)
}

func TestInvalidEventType(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	campaignID := uuid.New()
	_, err := pool.Exec(ctx, "INSERT INTO campaigns (id, name, status) VALUES ($1, $2, $3)", campaignID, "Constraint Test", "ACTIVE")
	require.NoError(t, err)

	_, err = pool.Exec(ctx,
		"INSERT INTO events (click_id, campaign_id, event_type, payload, created_date) VALUES ($1, $2, $3, $4, CURRENT_DATE)",
		uuid.New().String(), campaignID, "invalid_type", "{}")

	assert.Error(t, err)
}
