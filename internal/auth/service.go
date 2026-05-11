package auth

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth/crypto"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth/repository"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth/token"
)

var (
	ErrUserAlreadyExists  = errors.New("user already exists")
	ErrInvalidCredentials = errors.New("invalid credentials")
)

type Metrics struct {
	FailedLoginsTotal  atomic.Uint64
	InvalidCredentials atomic.Uint64
	TokenErrors        atomic.Uint64
}

func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		FailedLoginsTotal:  m.FailedLoginsTotal.Load(),
		InvalidCredentials: m.InvalidCredentials.Load(),
		TokenErrors:        m.TokenErrors.Load(),
	}
}

type MetricsSnapshot struct {
	FailedLoginsTotal  uint64
	InvalidCredentials uint64
	TokenErrors        uint64
}

type Service struct {
	repo       repository.Querier
	tokenMaker token.Maker
	hasher     *crypto.PasswordHasher
	metrics    Metrics
}

func NewService(repo repository.Querier, tokenMaker token.Maker, hasher *crypto.PasswordHasher) *Service {
	return &Service{
		repo:       repo,
		tokenMaker: tokenMaker,
		hasher:     hasher,
		metrics:    Metrics{},
	}
}

func (s *Service) GetMetrics() MetricsSnapshot {
	return s.metrics.Snapshot()
}

type RegisterRequest struct {
	Email      string
	Password   string
	Role       string
	CustomerID uuid.UUID
}

func (s *Service) Register(ctx context.Context, req RegisterRequest) (uuid.UUID, error) {
	hashedPassword, err := s.hasher.HashPassword(req.Password)
	if err != nil {
		return uuid.Nil, err
	}

	arg := repository.CreateUserParams{
		Email:        req.Email,
		PasswordHash: hashedPassword,
		Role:         req.Role,
	}
	if req.CustomerID != uuid.Nil {
		arg.CustomerID.Bytes = req.CustomerID
		arg.CustomerID.Valid = true
	}

	user, err := s.repo.CreateUser(ctx, arg)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: %v", ErrUserAlreadyExists, err)
	}

	return uuid.UUID(user.ID.Bytes), nil
}

type LoginResponse struct {
	AccessToken string
	User        repository.User
}

func (s *Service) Login(ctx context.Context, email, password string, duration time.Duration) (LoginResponse, error) {
	var hashToVerify string
	var userFound bool

	user, err := s.repo.GetUserByEmail(ctx, email)
	if err == nil {
		hashToVerify = user.PasswordHash
		userFound = true
	} else {
		hashToVerify = crypto.DummyHash
		userFound = false
	}

	match, err := crypto.VerifyPassword(password, hashToVerify)

	if !userFound || err != nil || !match {
		s.metrics.InvalidCredentials.Add(1)
		s.metrics.FailedLoginsTotal.Add(1)
		return LoginResponse{}, ErrInvalidCredentials
	}

	accessToken, err := s.tokenMaker.CreateToken(
		uuid.UUID(user.ID.Bytes),
		user.Role,
		uuid.UUID(user.CustomerID.Bytes),
		duration,
	)
	if err != nil {
		s.metrics.TokenErrors.Add(1)
		s.metrics.FailedLoginsTotal.Add(1)
		return LoginResponse{}, err
	}

	return LoginResponse{
		AccessToken: accessToken,
		User:        user,
	}, nil
}

func (s *Service) VerifyToken(ctx context.Context, accessToken string) (repository.User, error) {
	payload, err := s.tokenMaker.VerifyToken(accessToken)
	if err != nil {
		s.metrics.TokenErrors.Add(1)
		return repository.User{}, err
	}

	user, err := s.repo.GetUserByID(ctx, pgtype.UUID{Bytes: payload.UserID, Valid: true})
	if err != nil {
		return repository.User{}, err
	}

	return user, nil
}
