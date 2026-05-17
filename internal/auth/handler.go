package auth

import (
	"github.com/mykhailov-ua/ad-event-processor/internal/auth/pb"
)

import (
	"context"
	"errors"
	"strings"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Handler struct {
	pb.UnimplementedAuthServiceServer
	service *Service
	cfg     *config.Config
}

func NewHandler(service *Service, cfg *config.Config) *Handler {
	return &Handler{
		service: service,
		cfg:     cfg,
	}
}

// extractClientIP resolves the origin IP by evaluating X-Forwarded-For headers against a whitelist of trusted proxies.
// This prevents IP spoofing attacks when the service is deployed behind edge load balancers.
func (h *Handler) extractClientIP(ctx context.Context) string {
	peerIP := "unknown"
	if p, ok := peer.FromContext(ctx); ok {
		parts := strings.Split(p.Addr.String(), ":")
		if len(parts) > 0 {
			peerIP = parts[0]
		}
	}

	isTrusted := false
	for _, tp := range h.cfg.TrustedProxies {
		if tp != "" && peerIP == tp {
			isTrusted = true
			break
		}
	}

	// Trust loopback addresses to facilitate local testing and sidecar proxy deployments.
	if peerIP == "127.0.0.1" || peerIP == "::1" || peerIP == "bufconn" {
		isTrusted = true
	}

	if isTrusted {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if xff := md.Get("x-forwarded-for"); len(xff) > 0 {
				ips := strings.Split(xff[0], ",")
				if len(ips) > 0 && strings.TrimSpace(ips[0]) != "" {
					return strings.TrimSpace(ips[0])
				}
			}
			if xri := md.Get("x-real-ip"); len(xri) > 0 && xri[0] != "" {
				return xri[0]
			}
		}
	}

	return peerIP
}

func (h *Handler) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	customerID, _ := uuid.Parse(req.CustomerID)
	id, err := h.service.Register(ctx, RegisterDTO{
		Email:      req.Email,
		Password:   req.Password,
		Role:       req.Role,
		CustomerID: customerID,
	})
	if err != nil {
		return nil, mapError(err)
	}

	return &pb.RegisterResponse{
		UserId: id.String(),
	}, nil
}

func (h *Handler) Login(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResponse, error) {
	duration := time.Duration(req.DurationHours) * time.Hour
	if duration == 0 {
		duration = time.Duration(h.cfg.DefaultTokenDurationHrs) * time.Hour
	}

	clientIP := h.extractClientIP(ctx)

	userAgent := "grpc-client"
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if ua := md.Get("user-agent"); len(ua) > 0 {
			userAgent = ua[0]
		}
	}

	resp, err := h.service.Login(ctx, req.Email, req.Password, userAgent, clientIP, duration)
	if err != nil {
		return nil, mapError(err)
	}

	return &resp, nil
}

func (h *Handler) VerifyToken(ctx context.Context, req *pb.VerifyTokenRequest) (*pb.VerifyTokenResponse, error) {
	user, err := h.service.VerifyToken(ctx, req.AccessToken)
	if err != nil {
		return nil, mapError(err)
	}

	return &pb.VerifyTokenResponse{
		User: &pb.User{
			ID:         uuid.UUID(user.ID.Bytes).String(),
			Email:      user.Email,
			Role:       user.Role,
			CustomerID: uuid.UUID(user.CustomerID.Bytes).String(),
			CreatedAt:  timestamppb.New(user.CreatedAt.Time),
		},
	}, nil
}

func (h *Handler) RefreshToken(ctx context.Context, req *pb.RefreshTokenRequest) (*pb.RefreshTokenResponse, error) {
	duration := time.Duration(h.cfg.DefaultTokenDurationHrs) * time.Hour
	accessToken, refreshToken, err := h.service.RefreshToken(ctx, req.RefreshToken, duration)
	if err != nil {
		return nil, mapError(err)
	}
	return &pb.RefreshTokenResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
	}, nil
}

func (h *Handler) RevokeToken(ctx context.Context, req *pb.RevokeTokenRequest) (*pb.RevokeTokenResponse, error) {
	err := h.service.RevokeToken(ctx, req.RefreshToken)
	if err != nil {
		return nil, mapError(err)
	}
	return &pb.RevokeTokenResponse{}, nil
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrRateLimitExceeded) {
		return status.Errorf(codes.ResourceExhausted, "%v", err)
	}
	if errors.Is(err, ErrInvalidCredentials) || errors.Is(err, ErrInvalidToken) || errors.Is(err, ErrExpiredToken) || errors.Is(err, ErrAccountLocked) || errors.Is(err, ErrSessionBlocked) {
		return status.Errorf(codes.Unauthenticated, "%v", err)
	}
	if errors.Is(err, ErrUserAlreadyExists) {
		return status.Errorf(codes.AlreadyExists, "%v", err)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return status.Errorf(codes.NotFound, "user not found")
	}
	if errors.Is(err, ErrValidation) {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}
	return status.Errorf(codes.Internal, "internal server error: %v", err)
}
