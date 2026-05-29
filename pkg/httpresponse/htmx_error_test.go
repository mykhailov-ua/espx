package httpresponse

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHTMXError_Rendering(t *testing.T) {
	t.Run("TraditionalFullPageResponse", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/some-page", nil)
		rec := httptest.NewRecorder()

		HTMXError(rec, req, http.StatusNotFound, "NOT_FOUND", "Campaign does not exist")

		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.Equal(t, "text/html; charset=utf-8", rec.Header().Get("Content-Type"))

		body := rec.Body.String()
		assert.Contains(t, body, "<!DOCTYPE html>")
		assert.Contains(t, body, "NOT_FOUND")
		assert.Contains(t, body, "Campaign does not exist")
		assert.Contains(t, body, "404")
		assert.Contains(t, body, "Return to Safety")
	})

	t.Run("HTMXFragmentResponse", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/some-form", nil)
		req.Header.Set("HX-Request", "true")
		rec := httptest.NewRecorder()

		HTMXError(rec, req, http.StatusBadRequest, "BAD_REQUEST", "Limit must be a positive integer")

		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Equal(t, "text/html; charset=utf-8", rec.Header().Get("Content-Type"))

		body := rec.Body.String()
		assert.NotContains(t, body, "<!DOCTYPE html>")
		assert.Contains(t, body, "Error 400 (BAD_REQUEST)")
		assert.Contains(t, body, "Limit must be a positive integer")
		assert.Contains(t, body, "role=\"alert\"")
	})
}
