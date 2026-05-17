package management

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCSRFMiddleware(t *testing.T) {
	mdl := NewCSRFMiddleware()

	dummyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	handler := mdl(dummyHandler)

	token := GenerateSecureToken(32)

	t.Run("GET_Request_Allowed", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/admin/customers", nil)
		resp := httptest.NewRecorder()

		handler.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)
	})

	t.Run("POST_Login_AllowedWithoutToken", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "/api/v1/auth/login", nil)
		resp := httptest.NewRecorder()

		handler.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)
	})

	t.Run("POST_MissingCookie_Forbidden", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "/admin/customers", nil)
		resp := httptest.NewRecorder()

		handler.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusForbidden, resp.Code)
	})

	t.Run("POST_MissingHeader_Forbidden", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "/admin/customers", nil)
		req.AddCookie(&http.Cookie{Name: "csrfToken", Value: token})
		resp := httptest.NewRecorder()

		handler.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusForbidden, resp.Code)
	})

	t.Run("POST_MismatchToken_Forbidden", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "/admin/customers", nil)
		req.AddCookie(&http.Cookie{Name: "csrfToken", Value: token})
		req.Header.Set("X-CSRF-Token", "wrong-token")
		resp := httptest.NewRecorder()

		handler.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusForbidden, resp.Code)
	})

	t.Run("POST_ValidCSRF_Success", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "/admin/customers", nil)
		req.AddCookie(&http.Cookie{Name: "csrfToken", Value: token})
		req.Header.Set("X-CSRF-Token", token)
		resp := httptest.NewRecorder()

		handler.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)
	})
}
