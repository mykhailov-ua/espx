package management

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"espx/internal/auth"
	"espx/internal/auth/pb"
	"espx/internal/config"
	"espx/pkg/httpresponse"
	"github.com/redis/go-redis/v9"
)

var bufferPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

func putBuffer(buf *bytes.Buffer) {
	if buf == nil || buf.Cap() > 64*1024 {
		return
	}
	buf.Reset()
	bufferPool.Put(buf)
}

type AuthHandler struct {
	authClient pb.AuthServiceClient
	tokenMaker auth.Maker
	rdb        redis.UniversalClient
	cfg        *config.Config
}

func NewAuthHandler(authClient pb.AuthServiceClient, tokenMaker auth.Maker, rdb redis.UniversalClient, cfg *config.Config) *AuthHandler {
	return &AuthHandler{
		authClient: authClient,
		tokenMaker: tokenMaker,
		rdb:        rdb,
		cfg:        cfg,
	}
}

func (h *AuthHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/auth/login", h.login)
	mux.HandleFunc("POST /api/v1/auth/logout", h.logout)
	mux.HandleFunc("POST /api/v1/auth/refresh", h.refresh)
	mux.HandleFunc("GET /api/v1/auth/me", h.me)
	mux.HandleFunc("POST /api/v1/auth/register", h.register)
}

func setCookie(w http.ResponseWriter, name, value, path string, maxAge int, httpOnly bool) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    value,
		Path:     path,
		MaxAge:   maxAge,
		HttpOnly: httpOnly,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
	})
}

type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type UserDTO struct {
	ID          string   `json:"id"`
	Email       string   `json:"email,omitempty"`
	Role        string   `json:"role"`
	CustomerID  string   `json:"customer_id"`
	Permissions []string `json:"permissions,omitempty"`
}

func (h *AuthHandler) login(w http.ResponseWriter, r *http.Request) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer putBuffer(buf)

	if _, err := io.Copy(buf, r.Body); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "failed to read request body")
		return
	}

	var req LoginRequest
	if err := json.Unmarshal(buf.Bytes(), &req); err != nil || req.Email == "" || req.Password == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid login request")
		return
	}

	resp, err := h.authClient.Login(r.Context(), &pb.LoginRequest{
		Email:         req.Email,
		Password:      req.Password,
		DurationHours: 1,
	})
	if err != nil {
		slog.Warn("login failed", "email", req.Email, "error", err)
		httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "invalid credentials")
		return
	}

	setCookie(w, "accessToken", resp.AccessToken, "/", 3600, true)
	setCookie(w, "refreshToken", resp.RefreshToken, "/api/v1/auth", 30*24*3600, true)
	csrf, err := GenerateSecureToken(32)
	if err != nil {
		slog.Error("failed to generate secure csrf token due to entropy starvation", "error", err)
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", "internal system failure")
		return
	}
	setCookie(w, "csrfToken", csrf, "/", 3600, false)
	w.Header().Set("X-CSRF-Token", csrf)

	userDTO := UserDTO{
		ID:          resp.User.Id,
		Email:       resp.User.Email,
		Role:        resp.User.Role,
		CustomerID:  resp.User.CustomerId,
		Permissions: GetPermissionsForRole(resp.User.Role),
	}

	httpresponse.JSON(w, http.StatusOK, map[string]any{"user": userDTO})
}

func (h *AuthHandler) logout(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("refreshToken")
	if err == nil && cookie.Value != "" {
		if _, errRevoke := h.authClient.RevokeToken(r.Context(), &pb.RevokeTokenRequest{
			RefreshToken: cookie.Value,
		}); errRevoke != nil {
			slog.Warn("failed to revoke token on logout", "error", errRevoke)
		}
	}

	accessCookie, err := r.Cookie("accessToken")
	if err == nil && accessCookie.Value != "" && h.rdb != nil {
		payload, errPayload := h.tokenMaker.VerifyToken(accessCookie.Value)
		if errPayload == nil {
			pipe := h.rdb.Pipeline()
			ttl := time.Until(payload.ExpiredAt)
			pipe.Set(r.Context(), "revoked:token:"+payload.ID.String(), "true", ttl)
			pipe.Set(r.Context(), "revoked:session:"+payload.SessionID.String(), "true", ttl)
			if _, errExec := pipe.Exec(r.Context()); errExec != nil {
				slog.Error("failed to execute pipeline during logout token revocation", "error", errExec)
			}
		}
	}

	setCookie(w, "accessToken", "", "/", -1, true)
	setCookie(w, "refreshToken", "", "/api/v1/auth", -1, true)
	setCookie(w, "csrfToken", "", "/", -1, false)
	httpresponse.JSON(w, http.StatusNoContent, nil)
}

func (h *AuthHandler) refresh(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("refreshToken")
	if err != nil || cookie.Value == "" {
		httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "missing refresh token")
		return
	}

	resp, err := h.authClient.RefreshToken(r.Context(), &pb.RefreshTokenRequest{
		RefreshToken: cookie.Value,
	})
	if err != nil {
		slog.Warn("refresh token failed", "error", err)
		httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "unauthorized")
		return
	}

	setCookie(w, "accessToken", resp.AccessToken, "/", 3600, true)
	setCookie(w, "refreshToken", resp.RefreshToken, "/api/v1/auth", 30*24*3600, true)

	httpresponse.JSON(w, http.StatusOK, map[string]string{"status": "refreshed"})
}

func (h *AuthHandler) me(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("accessToken")
	if err != nil || cookie.Value == "" {
		httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "unauthorized")
		return
	}

	payload, err := h.tokenMaker.VerifyToken(cookie.Value)
	if err != nil {
		httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "unauthorized")
		return
	}

	if h.rdb != nil {
		ctxRevoked, cancel := context.WithTimeout(r.Context(), 100*time.Millisecond)
		defer cancel()

		cmds, errPipe := h.rdb.Pipelined(ctxRevoked, func(pipe redis.Pipeliner) error {
			pipe.Exists(ctxRevoked, "revoked:token:"+payload.ID.String())
			pipe.Exists(ctxRevoked, "revoked:session:"+payload.SessionID.String())
			pipe.Exists(ctxRevoked, "revoked:user:"+payload.UserID.String())
			return nil
		})

		if errPipe == nil && len(cmds) == 3 {
			for _, cmd := range cmds {
				if exists, _ := cmd.(*redis.IntCmd).Result(); exists > 0 {
					httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "unauthorized: token revoked")
					return
				}
			}
		}
	}

	dto := UserDTO{
		ID:          payload.UserID.String(),
		Role:        payload.Role,
		CustomerID:  payload.CustomerID.String(),
		Permissions: GetPermissionsForRole(payload.Role),
	}

	httpresponse.JSON(w, http.StatusOK, dto)
}

type RegisterRequest struct {
	Email      string `json:"email"`
	Password   string `json:"password"`
	Role       string `json:"role"`
	CustomerID string `json:"customer_id,omitempty"`
}

func (h *AuthHandler) register(w http.ResponseWriter, r *http.Request) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer putBuffer(buf)

	if _, err := io.Copy(buf, r.Body); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "failed to read request body")
		return
	}

	var req RegisterRequest
	if err := json.Unmarshal(buf.Bytes(), &req); err != nil || req.Email == "" || req.Password == "" || req.Role == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid register request")
		return
	}

	resp, err := h.authClient.Register(r.Context(), &pb.RegisterRequest{
		Email:      req.Email,
		Password:   req.Password,
		Role:       req.Role,
		CustomerId: req.CustomerID,
	})
	if err != nil {
		slog.Warn("registration failed", "email", req.Email, "error", err)
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}

	httpresponse.JSON(w, http.StatusCreated, map[string]any{"user_id": resp.UserId})
}
