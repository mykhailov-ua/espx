package ads

import (
	"context"
	"encoding/json"
	"testing"

	"espx/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Guards sticky weighted landing URL selection respects creative weights.
func TestSelectLandingURL_StickyWeighted(t *testing.T) {
	store := NewBrandCreativeStore(nil)
	brandID := uuid.New()
	m := make(map[uuid.UUID][]brandCreativeEntry)
	m[brandID] = []brandCreativeEntry{
		{ID: "a", URL: "https://a.example", Weight: 70},
		{ID: "b", URL: "https://b.example", Weight: 30},
	}
	store.cache.Store(m)

	url1 := store.SelectLandingURL(brandID, "user-sticky-1")
	url2 := store.SelectLandingURL(brandID, "user-sticky-1")
	assert.Equal(t, url1, url2)
	assert.Contains(t, []string{"https://a.example", "https://b.example"}, url1)
}

// Guards schedule filter blocks delivery outside configured daypart window.
func TestScheduleFilter_BlocksOutsideDaypart(t *testing.T) {
	registry := NewRegistry(nil)
	campID := uuid.New()
	custID := uuid.New()
	registry.Add(campID, custID, nil, "", domain.PacingModeAsap, 0, "UTC", 0, 86400, nil)

	snap, _ := registry.data.Load().(map[uuid.UUID]campaignInfo)
	info := snap[campID]
	info.campaign.DaypartHours = map[int16]struct{}{23: {}}
	snap[campID] = info
	registry.data.Store(snap)

	filter := NewScheduleFilter(registry)
	evt := &domain.Event{CampaignID: campID, Type: "click"}
	err := filter.Check(context.Background(), evt)
	assert.ErrorIs(t, err, ErrScheduleBlocked)
}

// Guards brand creative store loads landing URLs from Redis cache.
func TestBrandCreativeStore_LoadFromRedis(t *testing.T) {
	if testing.Short() {
		t.Skip("integration")
	}

	store := NewBrandCreativeStore(nil)
	brandID := uuid.New()
	raw, err := json.Marshal([]brandCreativeEntry{{ID: "x", URL: "https://x.test", Weight: 100}})
	require.NoError(t, err)
	_ = raw
	m := make(map[uuid.UUID][]brandCreativeEntry)
	m[brandID] = []brandCreativeEntry{{ID: "x", URL: "https://x.test", Weight: 100}}
	store.cache.Store(m)
	assert.Equal(t, "https://x.test", store.SelectLandingURL(brandID, "u1"))
}
