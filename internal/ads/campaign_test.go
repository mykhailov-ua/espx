package ads

import (
	"context"
	"testing"
	"time"

	"espx/internal/ads/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MockRepo struct {
	db.Querier
	ids []pgtype.UUID
	err error
}

func (m *MockRepo) ListActiveCampaigns(ctx context.Context) ([]db.Campaign, error) {
	var res []db.Campaign
	for _, id := range m.ids {
		res = append(res, db.Campaign{
			ID:         id,
			CustomerID: id,
			Status:     db.CampaignStatusTypeACTIVE,
		})
	}
	return res, m.err
}

func TestRegistry_Sync(t *testing.T) {
	id1 := uuid.New()
	id2 := uuid.New()
	mock := &MockRepo{
		ids: []pgtype.UUID{
			{Bytes: id1, Valid: true},
			{Bytes: id2, Valid: true},
		},
	}

	r := NewRegistry(mock)
	count, err := r.Sync(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 2, count)
	assert.True(t, r.Exists(id1))
	assert.True(t, r.Exists(id2))
	assert.False(t, r.Exists(uuid.New()))
}

func TestRegistry_StartSync(t *testing.T) {
	id1 := uuid.New()
	mock := &MockRepo{
		ids: []pgtype.UUID{{Bytes: id1, Valid: true}},
	}

	r := NewRegistry(mock)
	ctx, cancel := context.WithCancel(context.Background())

	r.StartSync(ctx, 10*time.Millisecond)

	assert.Eventually(t, func() bool {
		return r.Exists(id1)
	}, 200*time.Millisecond, 20*time.Millisecond)

	cancel()
	r.Wait(ctx)
}
