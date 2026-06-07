package ads

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"espx/internal/ads/pb"
	"espx/internal/config"
	"espx/internal/domain"
	"github.com/google/uuid"
	"github.com/panjf2000/gnet/v2"
	"google.golang.org/protobuf/proto"
)

type mockRegistry struct{}

func (m *mockRegistry) Exists(id uuid.UUID) bool { return true }
func (m *mockRegistry) Add(id, customerID uuid.UUID, brandID *uuid.UUID, brandFcapKey string, pacingMode domain.PacingMode, dailyBudget int64, timezone string, freqLimit, freqWindow int32, targetCountries []string) {
}
func (m *mockRegistry) GetCustomerID(id uuid.UUID) (uuid.UUID, bool) { return uuid.Nil, true }

var (
	staticCampaignMu sync.RWMutex
	staticCampaign   = &domain.Campaign{CustomerID: uuid.Nil, Location: time.UTC}
	cachedMockCamp   atomic.Pointer[domain.Campaign]
)

func (m *mockRegistry) GetCampaign(id uuid.UUID) (*domain.Campaign, bool) {
	if got := cachedMockCamp.Load(); got != nil && got.ID == id {
		return got, true
	}

	staticCampaignMu.RLock()
	defer staticCampaignMu.RUnlock()

	cp := *staticCampaign
	cp.ID = id
	idStr := id.String()
	custStr := cp.CustomerID.String()
	cp.IDStr = idStr
	cp.IDStrAny = idStr
	cp.CustomerIDStr = custStr
	cp.CustomerIDStrAny = custStr

	cp.BudgetCampaignKey = "budget:campaign:" + idStr
	cp.CampaignSyncKey = "budget:sync:campaign:" + idStr
	cp.CustomerSyncKey = "budget:sync:customer:" + custStr
	if cp.BrandFcapKey != "" {
		cp.FcapKeyPrefix = cp.BrandFcapKey + ":u:"
	} else {
		cp.FcapKeyPrefix = "fcap:c:" + idStr + ":u:"
	}
	cp.DailySpendKeyPrefix = "budget:daily_spent:campaign:" + idStr + ":"

	cachedMockCamp.Store(&cp)
	return &cp, true
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
		CampaignId: []byte(uuid.NewString()),
		EventType:  []byte("click"),
		Metadata: &pb.EventMetadata{
			ClickId: []byte("test-click"),
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

var staticRemoteAddr = &net.TCPAddr{IP: net.IPv4(1, 1, 1, 1), Port: 1234}

type mockGnetConn struct {
	gnet.Conn
	written []byte
	ctx     any
}

func (m *mockGnetConn) Context() any     { return m.ctx }
func (m *mockGnetConn) SetContext(v any) { m.ctx = v }

func (m *mockGnetConn) Write(b []byte) (int, error) {
	m.written = append(m.written[:0], b...)
	return len(b), nil
}

func (m *mockGnetConn) RemoteAddr() net.Addr {
	return staticRemoteAddr
}

func BenchmarkAdsPacketHandlerJSON(b *testing.B) {
	cfg := &config.Config{
		MaxRequestBodySize: 1024 * 1024,
	}
	registry := &mockRegistry{}
	sharder := NewJumpHashSharder(1)
	handler := NewAdsPacketHandler(cfg, registry, nil, nil, nil, sharder, "fraud-stream")

	payload := []byte(`{"campaign_id":"` + uuid.NewString() + `","user_id":"user123","type":"click","click_id":"click123","payload":{}}`)
	req := parsedHTTPRequest{
		Method:           []byte("POST"),
		Path:             []byte("/track"),
		ContentType:      []byte("application/json"),
		ClientIP:         []byte("1.1.1.1"),
		UserAgent:        []byte("Mozilla/5.0"),
		Body:             payload,
		ContentLength:    len(payload),
		HasContentLength: true,
	}

	conn := &mockGnetConn{written: make([]byte, 0, 512)}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.React(req, conn)
	}
}

func BenchmarkAdsPacketHandlerProto(b *testing.B) {
	cfg := &config.Config{
		MaxRequestBodySize: 1024 * 1024,
	}
	registry := &mockRegistry{}
	sharder := NewJumpHashSharder(1)
	handler := NewAdsPacketHandler(cfg, registry, nil, nil, nil, sharder, "fraud-stream")

	pbPayload := &pb.AdEvent{
		CampaignId: []byte(uuid.NewString()),
		EventType:  []byte("click"),
		Metadata: &pb.EventMetadata{
			ClickId: []byte("test-click"),
		},
	}
	body, _ := proto.Marshal(pbPayload)
	req := parsedHTTPRequest{
		Method:           []byte("POST"),
		Path:             []byte("/track"),
		ContentType:      []byte("application/x-protobuf"),
		ClientIP:         []byte("1.1.1.1"),
		UserAgent:        []byte("Mozilla/5.0"),
		Body:             body,
		ContentLength:    len(body),
		HasContentLength: true,
	}

	conn := &mockGnetConn{written: make([]byte, 0, 512)}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.React(req, conn)
	}
}
