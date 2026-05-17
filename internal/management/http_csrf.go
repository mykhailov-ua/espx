package management

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net/http"

	"github.com/mykhailov-ua/ad-event-processor/pkg/httpresponse"
)

// GenerateSecureToken generates unpredictable session secrets using hardware-backed cryptographic entropy. This prevents session guessing and token forgery attacks across distributed gateways.
func GenerateSecureToken(length int) string {
	b := make([]byte, length)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// NewCSRFMiddleware implements the Double Submit Cookie pattern to guard against Cross-Site Request Forgery on state-mutating HTTP endpoints. Requiring both a non-HttpOnly cookie and a matching custom HTTP header verifies that requests originate strictly from authenticated client scripts.
func NewCSRFMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodDelete {
				if r.URL.Path == "/api/v1/auth/login" || r.URL.Path == "/api/v1/auth/refresh" || r.URL.Path == "/api/v1/auth/logout" {
					next.ServeHTTP(w, r)
					return
				}

				cookie, err := r.Cookie("csrfToken")
				if err != nil || cookie.Value == "" {
					httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden: missing csrf cookie")
					return
				}

				headerToken := r.Header.Get("X-CSRF-Token")
				if headerToken == "" {
					httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden: missing csrf header")
					return
				}

				if subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(headerToken)) != 1 {
					httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden: invalid csrf token")
					return
				}
			}

			next.ServeHTTP(w, r)
		})
	}
}
