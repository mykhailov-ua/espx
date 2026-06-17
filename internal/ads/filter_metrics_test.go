package ads

import (
	"context"
	"testing"

	"espx/internal/domain"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// Guards geo lookup failures increment telemetry without blocking the event.
func TestGeoFilter_lookupErrorIncrementsCounter(t *testing.T) {
	before := testutil.ToFloat64(filterGeoLookupErrors)
	campID := uuid.New()
	cachedMockCamp.Store(&domain.Campaign{
		ID:              campID,
		TargetCountries: map[string]struct{}{"US": {}},
	})
	t.Cleanup(func() { cachedMockCamp.Store(nil) })

	f := NewGeoFilter(errGeoProvider{}, &mockRegistry{})
	err := f.Check(context.Background(), &domain.Event{IP: "8.8.8.8", CampaignID: campID})
	require.NoError(t, err)
	require.Equal(t, before+1, testutil.ToFloat64(filterGeoLookupErrors))
}
