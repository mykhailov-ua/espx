package management

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/pkg/httpresponse"
	"github.com/redis/go-redis/v9"
)

type contextKey string

const UserContextKey contextKey = "authenticated_user"

type AuthenticatedUser struct {
	UserID     uuid.UUID
	Role       string
	CustomerID uuid.UUID
}

func GetUser(ctx context.Context) (AuthenticatedUser, bool) {
	u, ok := ctx.Value(UserContextKey).(AuthenticatedUser)
	return u, ok
}

// AuthMiddleware enforces RBAC and session revocation policies across the gateway.
// Verification uses in-memory cryptographic evaluation to eliminate network round-trips on the hot path,
// coupled with a fast Redis revocation lookup protected by a 100ms circuit breaker.
type AuthMiddleware struct {
	tokenMaker auth.Maker
	rdb        redis.UniversalClient
	cfg        *config.Config
}

func NewAuthMiddleware(tokenMaker auth.Maker, rdb redis.UniversalClient, cfg *config.Config) *AuthMiddleware {
	return &AuthMiddleware{
		tokenMaker: tokenMaker,
		rdb:        rdb,
		cfg:        cfg,
	}
}

func (m *AuthMiddleware) RequireAuth(allowedRoles ...string) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if key := r.Header.Get("X-Admin-API-Key"); key != "" && m.cfg != nil && key == string(m.cfg.AdminAPIKey) {
				user := AuthenticatedUser{
					UserID:     uuid.Nil,
					Role:       "SA",
					CustomerID: uuid.Nil,
				}
				ctx := context.WithValue(r.Context(), UserContextKey, user)
				next(w, r.WithContext(ctx))
				return
			}

			cookie, err := r.Cookie("accessToken")
			if err != nil || cookie.Value == "" {
				httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "unauthorized: missing token")
				return
			}

			payload, err := m.tokenMaker.VerifyToken(cookie.Value)
			if err != nil {
				httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "unauthorized: invalid token")
				return
			}

			if m.rdb != nil {
				ctxTimeout, cancel := context.WithTimeout(r.Context(), 100*time.Millisecond)
				defer cancel()
				
				cmds, errPipe := m.rdb.Pipelined(ctxTimeout, func(pipe redis.Pipeliner) error {
					pipe.Exists(ctxTimeout, "revoked:token:"+payload.ID.String())
					pipe.Exists(ctxTimeout, "revoked:session:"+payload.SessionID.String())
					pipe.Exists(ctxTimeout, "revoked:user:"+payload.UserID.String())
					return nil
				})

				if errPipe != nil {
					slog.Error("redis revocation check failed, blocking request to prevent security bypass", "error", errPipe)
					httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "internal server error: security subsystem unavailable")
					return
				}
				
				for _, cmd := range cmds {
					if exists, _ := cmd.(*redis.IntCmd).Result(); exists > 0 {
						httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "unauthorized: session revoked")
						return
					}
				}
			}

			userRole := strings.ToUpper(payload.Role)
			if userRole == "SUPERADMIN" || userRole == "ADMIN" || userRole == "SA" {
				userRole = "SA"
			} else if userRole == "MANAGER" || userRole == "M" {
				userRole = "M"
			} else if userRole == "CUSTOMER" || userRole == "USER" || userRole == "C" {
				userRole = "C"
			} else if userRole == "GUEST" || userRole == "G" {
				userRole = "G"
			}

			roleAllowed := false
			for _, allowed := range allowedRoles {
				allowedClean := strings.ToUpper(allowed)
				if userRole == allowedClean || userRole == "SA" {
					roleAllowed = true
					break
				}
			}

			if !roleAllowed {
				httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden: insufficient permissions")
				return
			}

			user := AuthenticatedUser{
				UserID:     payload.UserID,
				Role:       userRole,
				CustomerID: payload.CustomerID,
			}
			ctx := context.WithValue(r.Context(), UserContextKey, user)
			next(w, r.WithContext(ctx))
		}
	}
}
