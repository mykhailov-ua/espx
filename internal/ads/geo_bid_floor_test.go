package ads

import (
	"context"
	"testing"
	"time"

	"espx/internal/domain"
	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestParseBidMicro(t *testing.T) {
	tests := []struct {
		name     string
		payload  string
		expected int64
	}{
		{"Valid bid_micro", `{"bid_micro": 1500000, "pub_id": "1"}`, 1500000},
		{"Spaces and tabs", `{"bid_micro" 	: 	2500000}`, 2500000},
		{"Missing key", `{"pub_id": "1"}`, 0},
		{"Zero bid_micro", `{"bid_micro": 0}`, 0},
		{"Negative value", `{"bid_micro": -100}`, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := parseBidMicro([]byte(tc.payload))
			assert.Equal(t, tc.expected, got)
		})
	}
}

func BenchmarkParseBidMicro(b *testing.B) {
	payload := []byte(`{"bid_micro": 1500000, "pub_id": "pub-12345"}`)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = parseBidMicro(payload)
	}
}

func TestUnifiedFilter_GeoBidFloor(t *testing.T) {
	campID := uuid.New()
	reg := &mockRegistry{}

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	f := NewUnifiedFilter(
		[]redis.UniversalClient{rdb},
		NewJumpHashSharder(1),
		reg,
		nil,
		100,
		time.Minute,
		time.Hour,
		time.Hour,
		100_000,
		10_000,
		"events-stream",
		10000,
	)

	geo := &MockGeoProvider{
		Countries: map[string]string{
			"2.2.2.2": "UA",
			"3.3.3.3": "US",
			"":        "",
		},
	}
	f.SetGeoProvider(geo)
	f.SetGeoBidFloor("UA", 1500000)
	f.SetGeoBidFloor("US", 3000000)

	ctx := context.Background()

	evt1 := &domain.Event{
		CampaignID: campID,
		IP:         "2.2.2.2",
		Payload:    []byte(`{"bid_micro": 1000000}`),
		UserID:     "user1",
	}
	err := f.Check(ctx, evt1)
	assert.ErrorIs(t, err, ErrBidFloorNotMet)

	evt2 := &domain.Event{
		CampaignID: campID,
		IP:         "2.2.2.2",
		Payload:    []byte(`{"bid_micro": 2000000}`),
		UserID:     "user1",
	}
	err = f.Check(ctx, evt2)
	assert.NotErrorIs(t, err, ErrBidFloorNotMet)

	evt3 := &domain.Event{
		CampaignID: campID,
		IP:         "4.4.4.4",
		Payload:    []byte(`{"bid_micro": 100000}`),
		UserID:     "user1",
	}
	geo.Countries["4.4.4.4"] = "DE"
	err = f.Check(ctx, evt3)
	assert.NotErrorIs(t, err, ErrBidFloorNotMet)

	evtEmptyIP := &domain.Event{
		CampaignID: campID,
		IP:         "",
		Payload:    []byte(`{"bid_micro": 100000}`),
		UserID:     "user1",
	}
	err = f.Check(ctx, evtEmptyIP)
	assert.NotErrorIs(t, err, ErrBidFloorNotMet)

	evtEmptyPayload := &domain.Event{
		CampaignID: campID,
		IP:         "2.2.2.2",
		Payload:    nil,
		UserID:     "user1",
	}
	err = f.Check(ctx, evtEmptyPayload)
	assert.ErrorIs(t, err, ErrBidFloorNotMet)

	evtMalformed := &domain.Event{
		CampaignID: campID,
		IP:         "2.2.2.2",
		Payload:    []byte(`{bid_micro: 1500000`),
		UserID:     "user1",
	}
	err = f.Check(ctx, evtMalformed)
	assert.ErrorIs(t, err, ErrBidFloorNotMet)
}
