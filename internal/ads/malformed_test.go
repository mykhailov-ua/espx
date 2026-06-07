package ads

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"espx/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestTrackHandlerMalformed(t *testing.T) {
	cfg := &config.Config{
		MaxRequestBodySize: 1024,
	}
	registry := &mockRegistry{}
	sharder := NewJumpHashSharder(1)
	handler := NewRouter(cfg, registry, nil, nil, nil, sharder, "fraud-stream")

	t.Run("Malformed Protobuf", func(t *testing.T) {
		body := []byte{0xFF, 0xEE, 0xDD}
		req := httptest.NewRequest("POST", "/track", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/x-protobuf")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("Payload Too Large", func(t *testing.T) {
		body := make([]byte, 2048)
		req := httptest.NewRequest("POST", "/track", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/x-protobuf")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("Invalid db.Campaign ID", func(t *testing.T) {

		body := []byte{10, 3, 104, 105, 33}
		req := httptest.NewRequest("POST", "/track", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/x-protobuf")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
}
