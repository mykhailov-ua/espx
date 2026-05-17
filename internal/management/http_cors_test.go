package management

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCORSMiddleware(t *testing.T) {
	origins := []string{"https://dashboard.example.com", "http://localhost:8188"}
	mdl := NewCORSMiddleware(origins)

	dummyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	handler := mdl(dummyHandler)

	t.Run("AllowedOrigin_OPTIONS", func(t *testing.T) {
		req, _ := http.NewRequest("OPTIONS", "/admin/customers", nil)
		req.Header.Set("Origin", "https://dashboard.example.com")
		resp := httptest.NewRecorder()

		handler.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, "https://dashboard.example.com", resp.Header().Get("Access-Control-Allow-Origin"))
		assert.Equal(t, "true", resp.Header().Get("Access-Control-Allow-Credentials"))
		assert.Equal(t, "Origin", resp.Header().Get("Vary"))
		assert.Contains(t, resp.Header().Get("Access-Control-Allow-Methods"), "OPTIONS")
		assert.Contains(t, resp.Header().Get("Access-Control-Allow-Headers"), "X-CSRF-Token")
	})

	t.Run("AllowedOrigin_POST", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "/admin/customers", nil)
		req.Header.Set("Origin", "http://localhost:8188")
		resp := httptest.NewRecorder()

		handler.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, "http://localhost:8188", resp.Header().Get("Access-Control-Allow-Origin"))
		assert.Equal(t, "true", resp.Header().Get("Access-Control-Allow-Credentials"))
	})

	t.Run("DisallowedOrigin", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "/admin/customers", nil)
		req.Header.Set("Origin", "https://evil.com")
		resp := httptest.NewRecorder()

		handler.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Empty(t, resp.Header().Get("Access-Control-Allow-Origin"))
	})
}
