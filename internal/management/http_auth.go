package management

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/mykhailov-ua/ad-event-processor/internal/auth"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth/pb"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
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
}

func setCookie(w http.ResponseWriter, name, value, path string, maxAge int) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    value,
		Path:     path,
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
	})
}

type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type UserDTO struct {
	ID         string `json:"id"`
	Email      string `json:"email,omitempty"`
	Role       string `json:"role"`
	CustomerID string `json:"customer_id"`
}

func (h *AuthHandler) login(w http.ResponseWriter, r *http.Request) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer putBuffer(buf)

	if _, err := io.Copy(buf, r.Body); err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}

	var req LoginRequest
	if err := json.Unmarshal(buf.Bytes(), &req); err != nil || req.Email == "" || req.Password == "" {
		http.Error(w, "invalid login request", http.StatusBadRequest)
		return
	}

	resp, err := h.authClient.Login(r.Context(), &pb.LoginRequest{
		Email:         req.Email,
		Password:      req.Password,
		DurationHours: 1,
	})
	if err != nil {
		slog.Warn("login failed", "email", req.Email, "error", err)
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
		return
	}

	setCookie(w, "accessToken", resp.AccessToken, "/", 3600)
	setCookie(w, "refreshToken", resp.RefreshToken, "/api/v1/auth", 30*24*3600)

	userDTO := UserDTO{
		ID:         resp.User.ID,
		Email:      resp.User.Email,
		Role:       resp.User.Role,
		CustomerID: resp.User.CustomerID,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{"user": userDTO})
}

func (h *AuthHandler) logout(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("refreshToken")
	if err == nil && cookie.Value != "" {
		_, _ = h.authClient.RevokeToken(r.Context(), &pb.RevokeTokenRequest{
			RefreshToken: cookie.Value,
		})
	}

	accessCookie, err := r.Cookie("accessToken")
	if err == nil && accessCookie.Value != "" && h.rdb != nil {
		payload, errPayload := h.tokenMaker.VerifyToken(accessCookie.Value)
		if errPayload == nil {
			_ = h.rdb.Set(r.Context(), "revoked:token:"+payload.ID.String(), "true", time.Until(payload.ExpiredAt)).Err()
		}
	}

	setCookie(w, "accessToken", "", "/", -1)
	setCookie(w, "refreshToken", "", "/api/v1/auth", -1)
	w.WriteHeader(http.StatusNoContent)
}

func (h *AuthHandler) refresh(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("refreshToken")
	if err != nil || cookie.Value == "" {
		http.Error(w, "missing refresh token", http.StatusUnauthorized)
		return
	}

	resp, err := h.authClient.RefreshToken(r.Context(), &pb.RefreshTokenRequest{
		RefreshToken: cookie.Value,
	})
	if err != nil {
		slog.Warn("refresh token failed", "error", err)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	setCookie(w, "accessToken", resp.AccessToken, "/", 3600)
	setCookie(w, "refreshToken", resp.RefreshToken, "/api/v1/auth", 30*24*3600)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"refreshed"}`))
}

func (h *AuthHandler) me(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("accessToken")
	if err != nil || cookie.Value == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	payload, err := h.tokenMaker.VerifyToken(cookie.Value)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	if h.rdb != nil {
		revoked, _ := h.rdb.Exists(r.Context(), "revoked:token:"+payload.ID.String()).Result()
		if revoked > 0 {
			http.Error(w, "unauthorized: token revoked", http.StatusUnauthorized)
			return
		}
	}

	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer putBuffer(buf)

	dto := UserDTO{
		ID:         payload.UserID.String(),
		Role:       payload.Role,
		CustomerID: payload.CustomerID.String(),
	}

	_ = json.NewEncoder(buf).Encode(dto)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}
