package ads

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/pb"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"google.golang.org/protobuf/proto"
)

type mockRegistry struct{}

func (m *mockRegistry) Exists(id uuid.UUID) bool { return true }
func (m *mockRegistry) Add(id, customerID uuid.UUID, brandID *uuid.UUID, brandFcapKey string, pacingMode domain.PacingMode, dailyBudget int64, timezone string, freqLimit, freqWindow int32, targetCountries []string) {
}
func (m *mockRegistry) GetCustomerID(id uuid.UUID) (uuid.UUID, bool) { return uuid.Nil, true }
var staticCampaign = &domain.Campaign{CustomerID: uuid.Nil, Location: time.UTC}

func (m *mockRegistry) GetCampaign(id uuid.UUID) (*domain.Campaign, bool) {
	staticCampaign.ID = id
	return staticCampaign, true
}
func (m *mockRegistry) Sync(ctx context.Context) (int, error)                 { return 0, nil }
func (m *mockRegistry) StartSync(ctx context.Context, interval time.Duration) {}
func (m *mockRegistry) Wait(ctx context.Context) error                        { return nil }

type mockBody struct {
	*bytes.Reader
}

func (m mockBody) Close() error { return nil }

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
		req := httptest.NewRequest("POST", "/track", nil)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		reader := bytes.NewReader(body)
		req.Body = mockBody{Reader: reader}

		for pb.Next() {
			reader.Reset(body)
			w.Body.Reset()
			w.Code = 0

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
		req := httptest.NewRequest("POST", "/track", nil)
		req.Header.Set("Content-Type", "application/x-protobuf")
		w := httptest.NewRecorder()
		reader := bytes.NewReader(body)
		req.Body = mockBody{Reader: reader}

		for pb.Next() {
			reader.Reset(body)
			w.Body.Reset()
			w.Code = 0

			handler.ServeHTTP(w, req)
		}
	})
}

