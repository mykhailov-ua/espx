package management

import (
	"errors"
	"net/http"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

func TestMapServiceError(t *testing.T) {
	status, code, msg := mapServiceError(errors.New("customer not found"))
	assert.Equal(t, http.StatusNotFound, status)
	assert.Equal(t, "NOT_FOUND", code)
	assert.Equal(t, "resource not found", msg)

	status, code, msg = mapServiceError(errors.New("insufficient balance"))
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "BAD_REQUEST", code)
	assert.Equal(t, "insufficient balance", msg)

	status, code, msg = mapServiceError(pgx.ErrNoRows)
	assert.Equal(t, http.StatusNotFound, status)
	assert.Equal(t, "NOT_FOUND", code)
	assert.Equal(t, "resource not found", msg)

	status, code, msg = mapServiceError(errors.New("dial tcp: connection refused"))
	assert.Equal(t, http.StatusInternalServerError, status)
	assert.Equal(t, "INTERNAL_ERROR", code)
	assert.Equal(t, "internal error", msg)
}

func TestParseMoneyMicro(t *testing.T) {
	micro := int64(1_500_000)
	v, err := parseMoneyMicro(&micro, 0, false, "amount")
	assert.NoError(t, err)
	assert.Equal(t, int64(1_500_000), v)

	legacy := 2.5
	v, err = parseMoneyMicro(nil, legacy, true, "amount")
	assert.NoError(t, err)
	assert.Equal(t, int64(2_500_000), v)
}
