package ads

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
)

func BenchmarkGeoFilter(b *testing.B) {
	geo := &MockGeoProvider{}
	registry := &mockRegistry{}
	f := NewGeoFilter(geo, registry)
	evt := &domain.Event{
		IP:         "1.1.1.1",
		CampaignID: uuid.New(),
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f.Check(ctx, evt)
	}
}

func BenchmarkFraudFilter_DC(b *testing.B) {
	geo := &MockGeoProvider{}
	f := NewFraudFilter(geo, nil, 300*time.Millisecond)
	evt := &domain.Event{
		IP: "1.1.1.66", // Mock triggers DC fraud
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f.Check(ctx, evt)
	}
}
