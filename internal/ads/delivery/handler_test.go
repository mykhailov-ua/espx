package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/pb"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

type mockRegistry struct{}

func (m *mockRegistry) Exists(id uuid.UUID) bool                              { return true }
func (m *mockRegistry) Add(id, customerID uuid.UUID)                          {}
func (m *mockRegistry) GetCustomerID(id uuid.UUID) (uuid.UUID, bool)          { return uuid.Nil, true }
func (m *mockRegistry) Sync(ctx context.Context) (int, error)                 { return 0, nil }
func (m *mockRegistry) StartSync(ctx context.Context, interval time.Duration) {}
func (m *mockRegistry) Wait()                                                 {}

type mockRedis struct {
	redis.UniversalClient
}

func (m *mockRedis) XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx)
	cmd.SetVal("1-0")
	return cmd
}

func BenchmarkTrackHandlerJSON(b *testing.B) {
	cfg := &config.Config{
		MaxRequestBodySize: 1024 * 1024,
	}
	registry := &mockRegistry{}
	proc := ads.NewStreamConsumer(nil, &mockRedis{}, "s", "g", "c", 10, 1, 100*time.Millisecond, 1*time.Second, 1000, 10*time.Millisecond, 100*time.Millisecond, 3, 1*time.Minute)
	handler := NewRouter(cfg, registry, proc, nil)

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
	proc := ads.NewStreamConsumer(nil, &mockRedis{}, "s", "g", "c", 10, 1, 100*time.Millisecond, 1*time.Second, 1000, 10*time.Millisecond, 100*time.Millisecond, 3, 1*time.Minute)
	handler := NewRouter(cfg, registry, proc, nil)

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
