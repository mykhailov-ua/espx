package management

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthMiddleware_RequireAuth(t *testing.T) {
	cfg := &config.Config{
		TokenSymmetricKey: "01234567890123456789012345678901",
		AdminAPIKey:       "secret-api-key",
	}
	tokenMaker, err := auth.NewPasetoMaker(string(cfg.TokenSymmetricKey))
	require.NoError(t, err)

	m := NewAuthMiddleware(tokenMaker, nil, cfg)

	targetHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, ok := GetUser(r.Context())
		if !ok {
			http.Error(w, "user not in context", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("role:" + u.Role))
	})

	t.Run("APIKey_Success", func(t *testing.T) {
		handler := m.RequireAuth("SA")(targetHandler)

		req, _ := http.NewRequest("GET", "/protected", nil)
		req.Header.Set("X-Admin-API-Key", "secret-api-key")
		resp := httptest.NewRecorder()

		handler.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, "role:SA", resp.Body.String())
	})

	t.Run("ValidToken_AllowedRole", func(t *testing.T) {
		handler := m.RequireAuth("M", "SA")(targetHandler)

		token, _ := tokenMaker.CreateToken(uuid.New(), "manager", uuid.New(), time.Hour)
		req, _ := http.NewRequest("GET", "/protected", nil)
		req.AddCookie(&http.Cookie{Name: "accessToken", Value: token})
		resp := httptest.NewRecorder()

		handler.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, "role:M", resp.Body.String())
	})

	t.Run("ValidToken_ForbiddenRole", func(t *testing.T) {
		handler := m.RequireAuth("SA")(targetHandler)

		token, _ := tokenMaker.CreateToken(uuid.New(), "customer", uuid.New(), time.Hour)
		req, _ := http.NewRequest("GET", "/protected", nil)
		req.AddCookie(&http.Cookie{Name: "accessToken", Value: token})
		resp := httptest.NewRecorder()

		handler.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusForbidden, resp.Code)
	})

	t.Run("MissingToken", func(t *testing.T) {
		handler := m.RequireAuth("SA")(targetHandler)

		req, _ := http.NewRequest("GET", "/protected", nil)
		resp := httptest.NewRecorder()

		handler.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusUnauthorized, resp.Code)
	})

	t.Run("ExpiredToken", func(t *testing.T) {
		handler := m.RequireAuth("SA")(targetHandler)

		token, _ := tokenMaker.CreateToken(uuid.New(), "SA", uuid.New(), -time.Hour)
		req, _ := http.NewRequest("GET", "/protected", nil)
		req.AddCookie(&http.Cookie{Name: "accessToken", Value: token})
		resp := httptest.NewRecorder()

		handler.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusUnauthorized, resp.Code)
	})
}
