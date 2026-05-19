package ads

import (
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/pb"
)

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/proto"
)

type mockRegistry struct{}

func (m *mockRegistry) Exists(id uuid.UUID) bool { return true }
func (m *mockRegistry) Add(id, customerID uuid.UUID, pacingMode domain.PacingMode, dailyBudget decimal.Decimal, timezone string, freqLimit, freqWindow int32, targetCountries []string) {
}
func (m *mockRegistry) GetCustomerID(id uuid.UUID) (uuid.UUID, bool) { return uuid.Nil, true }
func (m *mockRegistry) GetCampaign(id uuid.UUID) (*domain.Campaign, bool) {
	return &domain.Campaign{ID: id, CustomerID: uuid.Nil}, true
}
func (m *mockRegistry) Sync(ctx context.Context) (int, error)                 { return 0, nil }
func (m *mockRegistry) StartSync(ctx context.Context, interval time.Duration) {}
func (m *mockRegistry) Wait(ctx context.Context) error                        { return nil }

func BenchmarkTrackHandlerJSON(b *testing.B) {
	cfg := &config.Config{
		MaxRequestBodySize: 1024 * 1024,
	}
	registry := &mockRegistry{}
	sharder := NewJumpHashSharder(1)
	handler := NewRouter(cfg, registry, nil, nil, nil, sharder, "fraud-stream")

	payload := map[string]interface{}{
		"campaign_id": uuid.New(),
		"type":        "click",
		"click_id":    "test-click",
		"payload":     map[string]string{"foo": "bar"},
	}
	body, _ := json.Marshal(payload)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req := httptest.NewRequest("POST", "/track", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
		}
	})
}

func BenchmarkTrackHandlerProto(b *testing.B) {
	cfg := &config.Config{
		MaxRequestBodySize: 1024 * 1024,
	}
	registry := &mockRegistry{}
	sharder := NewJumpHashSharder(1)
	handler := NewRouter(cfg, registry, nil, nil, nil, sharder, "fraud-stream")

	pbPayload := &pb.AdEvent{
		CampaignId: uuid.NewString(),
		EventType:  "click",
		Metadata: &pb.EventMetadata{
			ClickId: "test-click",
		},
	}
	body, _ := proto.Marshal(pbPayload)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req := httptest.NewRequest("POST", "/track", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/x-protobuf")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
		}
	})
}
