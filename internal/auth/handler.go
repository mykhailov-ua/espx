package auth

import (
	"espx/internal/auth/pb"
)

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"espx/internal/config"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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

func (h *Handler) extractClientIP(ctx context.Context) string {
	peerIP := "unknown"
	if p, ok := peer.FromContext(ctx); ok {
		host, _, err := net.SplitHostPort(p.Addr.String())
		if err == nil {
			peerIP = host
		} else {
			peerIP = p.Addr.String()
		}
	}

	isTrusted := false
	for _, tp := range h.cfg.TrustedProxies {
		if tp != "" && peerIP == tp {
			isTrusted = true
			break
		}
	}

	if peerIP == "127.0.0.1" || peerIP == "::1" || peerIP == "bufconn" {
		isTrusted = true
	}

	if isTrusted {
		if md, ok := metadata.FromIncomingContext(ctx); ok {

			if xri := md.Get("x-real-ip"); len(xri) > 0 && xri[0] != "" {
				return strings.TrimSpace(xri[0])
			}

			if xff := md.Get("x-forwarded-for"); len(xff) > 0 {
				ips := strings.Split(xff[0], ",")
				if len(ips) > 0 {
					val := strings.TrimSpace(ips[len(ips)-1])
					if val != "" {
						return val
					}
				}
			}
		}
	}

	return peerIP
}

func (h *Handler) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	var customerID uuid.UUID
	var err error
	if req.CustomerId != "" {
		customerID, err = uuid.Parse(req.CustomerId)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid customer id")
		}
	}
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
	if duration <= 0 {
		duration = time.Duration(h.cfg.DefaultTokenDurationHrs) * time.Hour
	} else if duration > 24*time.Hour {
		duration = 24 * time.Hour
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
			Id:         uuid.UUID(user.ID.Bytes).String(),
			Email:      user.Email,
			Role:       user.Role,
			CustomerId: uuid.UUID(user.CustomerID.Bytes).String(),
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
		return status.Error(codes.ResourceExhausted, err.Error())
	}
	if errors.Is(err, ErrInvalidCredentials) || errors.Is(err, ErrInvalidToken) || errors.Is(err, ErrExpiredToken) || errors.Is(err, ErrAccountLocked) || errors.Is(err, ErrSessionBlocked) {
		return status.Error(codes.Unauthenticated, err.Error())
	}
	if errors.Is(err, ErrUserAlreadyExists) {
		return status.Error(codes.AlreadyExists, err.Error())
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return status.Error(codes.NotFound, "user not found")
	}
	if errors.Is(err, ErrValidation) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Error(codes.Internal, "internal server error")
}
