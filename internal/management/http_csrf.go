package management

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"strings"

	"espx/pkg/httpresponse"
)

func GenerateSecureToken(length int) (string, error) {
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func NewCSRFMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodDelete {
				if !strings.HasPrefix(r.URL.Path, "/api/v1/") && !strings.HasPrefix(r.URL.Path, "/admin/") {
					next.ServeHTTP(w, r)
					return
				}

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
