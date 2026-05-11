package grpc

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth/pb"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth/token"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Handler struct {
	pb.UnimplementedAuthServiceServer
	service *auth.Service
	cfg     *config.Config
}

func NewHandler(service *auth.Service, cfg *config.Config) *Handler {
	return &Handler{
		service: service,
		cfg:     cfg,
	}
}

func (h *Handler) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	customerID, _ := uuid.Parse(req.CustomerId)
	id, err := h.service.Register(ctx, auth.RegisterRequest{
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

	resp, err := h.service.Login(ctx, req.Email, req.Password, duration)
	if err != nil {
		return nil, mapError(err)
	}

	return &pb.LoginResponse{
		AccessToken: resp.AccessToken,
		User: &pb.User{
			Id:         resp.User.ID.String(),
			Email:      resp.User.Email,
			Role:       resp.User.Role,
			CustomerId: resp.User.CustomerID.String(),
			CreatedAt:  timestamppb.New(resp.User.CreatedAt.Time),
		},
	}, nil
}

func (h *Handler) VerifyToken(ctx context.Context, req *pb.VerifyTokenRequest) (*pb.VerifyTokenResponse, error) {
	user, err := h.service.VerifyToken(ctx, req.AccessToken)
	if err != nil {
		return nil, mapError(err)
	}

	return &pb.VerifyTokenResponse{
		User: &pb.User{
			Id:         user.ID.String(),
			Email:      user.Email,
			Role:       user.Role,
			CustomerId: user.CustomerID.String(),
			CreatedAt:  timestamppb.New(user.CreatedAt.Time),
		},
	}, nil
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, auth.ErrInvalidCredentials) || errors.Is(err, token.ErrInvalidToken) || errors.Is(err, token.ErrExpiredToken) {
		return status.Errorf(codes.Unauthenticated, "%v", err)
	}
	if errors.Is(err, auth.ErrUserAlreadyExists) {
		return status.Errorf(codes.AlreadyExists, "%v", err)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return status.Errorf(codes.NotFound, "user not found")
	}
	return status.Errorf(codes.Internal, "internal server error: %v", err)
}
