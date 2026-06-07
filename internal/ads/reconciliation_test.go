package ads

import (
	"context"
	"errors"
	"testing"
	"time"

	"espx/internal/domain"
	"github.com/google/uuid"
	"github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"espx/internal/metrics"
)

type MockCampaignRepository struct {
	campaigns []*domain.Campaign
}

func (m *MockCampaignRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.Campaign, error) {
	for _, c := range m.campaigns {
		if c.ID == id {
			return c, nil
		}
	}
	return nil, errors.New("not found")
}

func (m *MockCampaignRepository) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.CampaignStatus) error {
	return nil
}

func (m *MockCampaignRepository) UpdateSpend(ctx context.Context, id uuid.UUID, amount int64, txID string) error {
	return nil
}

func (m *MockCampaignRepository) ListActive(ctx context.Context) ([]*domain.Campaign, error) {
	return m.campaigns, nil
}

func TestReconciliationWorker_DataDriftDetection(t *testing.T) {
	ctx := context.Background()

	campID1 := uuid.New()
	campID2 := uuid.New()

	repo := &MockCampaignRepository{
		campaigns: []*domain.Campaign{
			{ID: campID1, Status: domain.CampaignStatusActive},
			{ID: campID2, Status: domain.CampaignStatusActive},
		},
	}

	pg := &MockPostgresDB{
		spends: map[uuid.UUID]int64{
			campID1: 100_000,
			campID2: 200_000,
		},
	}
	pg.Healthy.Store(true)

	ch := &MockClickHouseDB{}

	logTime := time.Now().Add(-10 * time.Minute)

	for i := 0; i < 9; i++ {
		ch.LogEvent(&domain.Event{
			CampaignID: campID1,
			ClickID:    uuid.NewString(),
			Type:       "click",
			CreatedAt:  logTime,
		})
		ch.LogEvent(&domain.Event{
			CampaignID: campID1,
			ClickID:    uuid.NewString(),
			Type:       "impression",
			CreatedAt:  logTime,
		})
	}

	for i := 0; i < 18; i++ {
		ch.LogEvent(&domain.Event{
			CampaignID: campID2,
			ClickID:    uuid.NewString(),
			Type:       "click",
			CreatedAt:  logTime,
		})
	}
	for i := 0; i < 20; i++ {
		ch.LogEvent(&domain.Event{
			CampaignID: campID2,
			ClickID:    uuid.NewString(),
			Type:       "impression",
			CreatedAt:  logTime,
		})
	}

	rw := NewReconciliationWorker(pg, ch, repo, 0.005, 5*time.Minute, 10*time.Minute)

	err := rw.Reconcile(ctx)
	require.NoError(t, err)

	metric1 := &io_prometheus_client.Metric{}
	err = metrics.DataDriftRatio.WithLabelValues(campID1.String()).Write(metric1)
	require.NoError(t, err)
	driftVal1 := metric1.GetGauge().GetValue()

	metric2 := &io_prometheus_client.Metric{}
	err = metrics.DataDriftRatio.WithLabelValues(campID2.String()).Write(metric2)
	require.NoError(t, err)
	driftVal2 := metric2.GetGauge().GetValue()

	assert.InDelta(t, 0.01, driftVal1, 0.0001, "Campaign 1 should detect 1.0% drift due to lost batch")

	assert.Equal(t, 0.0, driftVal2, "Campaign 2 should have exactly 0.0% drift")
}
