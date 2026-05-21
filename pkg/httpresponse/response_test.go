package httpresponse

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJSON(t *testing.T) {
	rec := httptest.NewRecorder()
	data := map[string]string{"foo": "bar"}

	JSON(rec, http.StatusOK, data)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	assert.Equal(t, `{"foo":"bar"}`+"\n", rec.Body.String())
}

func TestJSON_NilData(t *testing.T) {
	rec := httptest.NewRecorder()

	JSON(rec, http.StatusNoContent, nil)

	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	assert.Empty(t, rec.Body.String())
}

func TestError(t *testing.T) {
	rec := httptest.NewRecorder()

	Error(rec, http.StatusBadRequest, "INVALID_INPUT", "missing field")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	expected := `{"error":{"code":"INVALID_INPUT","message":"missing field"}}`
	assert.Equal(t, expected+"\n", rec.Body.String())
}

func BenchmarkJSON(b *testing.B) {
	rec := httptest.NewRecorder()
	data := map[string]string{"foo": "bar"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		JSON(rec, http.StatusOK, data)
		rec.Body.Reset()
	}
}

func BenchmarkError(b *testing.B) {
	rec := httptest.NewRecorder()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Error(rec, http.StatusBadRequest, "INVALID_INPUT", "missing field")
		rec.Body.Reset()
	}
}

