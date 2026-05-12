package delivery

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestTrackHandlerMalformed(t *testing.T) {
	cfg := &config.Config{
		MaxRequestBodySize: 1024,
	}
	registry := &mockRegistry{}
	handler := NewRouter(cfg, registry, nil, nil, nil)

	t.Run("Malformed Protobuf", func(t *testing.T) {
		body := []byte{0xFF, 0xEE, 0xDD} // Injects a corrupted byte sequence to trigger Protobuf wire-format parsing errors.
		req := httptest.NewRequest("POST", "/track", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/x-protobuf")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("Payload Too Large", func(t *testing.T) {
		body := make([]byte, 2048) // Exceeds MaxRequestBodySize to verify the request body reader's enforcement of hard memory limits.
		req := httptest.NewRequest("POST", "/track", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/x-protobuf")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("Invalid Campaign ID", func(t *testing.T) {
		// Constructs a valid Protobuf structure containing a malformed UUID string to validate secondary logic-level checks.
		body := []byte{10, 3, 104, 105, 33} // tag 1 (CampaignId), len 3, val "hi!"
		req := httptest.NewRequest("POST", "/track", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/x-protobuf")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
}

type mockRedisPing struct {
	redis.UniversalClient
}

func (m *mockRedisPing) Ping(ctx context.Context) *redis.StatusCmd {
	return redis.NewStatusCmd(ctx, "PONG")
}
