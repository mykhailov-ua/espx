package management

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"espx/pkg/httpresponse"
	"github.com/jackc/pgx/v5"
)

// mapServiceError maps domain failures to stable client-facing codes without leaking store internals.
func mapServiceError(err error) (status int, code, message string) {
	if err == nil {
		return http.StatusOK, "", ""
	}
	if errors.Is(err, errForbidden) {
		return http.StatusForbidden, "FORBIDDEN", "forbidden"
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return http.StatusNotFound, "NOT_FOUND", "resource not found"
	}

	msg := err.Error()
	switch {
	case strings.Contains(msg, "not found"):
		return http.StatusNotFound, "NOT_FOUND", "resource not found"
	case strings.Contains(msg, "insufficient balance"):
		return http.StatusBadRequest, "BAD_REQUEST", "insufficient balance"
	case strings.Contains(msg, "belongs to another customer"),
		strings.Contains(msg, "cannot be paused"),
		strings.Contains(msg, "is not paused"),
		strings.Contains(msg, "outside scheduled delivery"),
		strings.Contains(msg, "invalid pacing mode"),
		strings.Contains(msg, "weight must be positive"),
		strings.Contains(msg, "status must be ACTIVE or PAUSED"),
		strings.Contains(msg, "incomplete idempotency"):
		return http.StatusBadRequest, "BAD_REQUEST", msg
	default:
		return http.StatusInternalServerError, "INTERNAL_ERROR", "internal error"
	}
}

// writeServiceError logs server failures and returns a sanitized HTTP error body.
func writeServiceError(w http.ResponseWriter, err error, logAttrs ...any) {
	status, code, message := mapServiceError(err)
	if status >= http.StatusInternalServerError {
		attrs := append([]any{slog.String("error", err.Error())}, logAttrs...)
		slog.Error("management request failed", attrs...)
	}
	httpresponse.Error(w, status, code, message)
}
