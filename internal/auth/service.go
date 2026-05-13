package auth

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth/crypto"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth/limiter"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth/repository"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth/token"
	"github.com/redis/go-redis/v9"
)

var (
	ErrUserAlreadyExists  = errors.New("user already exists")
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrAccountLocked      = errors.New("account locked due to too many failed attempts")
	ErrValidation         = errors.New("validation failed")
	ErrSessionBlocked     = errors.New("session is blocked")
)

var (
	emailRegex    = regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
	passwordRegex = regexp.MustCompile(`^[A-Za-z\d@$!%*?&]{8,}$`)
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
	repo       repository.Store
	tokenMaker token.Maker
	hasher     *crypto.PasswordHasher
	lockout    *limiter.LockoutLimiter
	rdb        redis.UniversalClient
	metrics    Metrics
}

func NewService(repo repository.Store, tokenMaker token.Maker, hasher *crypto.PasswordHasher, lockout *limiter.LockoutLimiter, rdb redis.UniversalClient) *Service {
	return &Service{
		repo:       repo,
		tokenMaker: tokenMaker,
		hasher:     hasher,
		lockout:    lockout,
		rdb:        rdb,
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
	req.Email = strings.ToLower(strings.TrimSpace(req.Email))
	if !emailRegex.MatchString(req.Email) {
		return uuid.Nil, fmt.Errorf("%w: invalid email format", ErrValidation)
	}
	if !passwordRegex.MatchString(req.Password) {
		return uuid.Nil, fmt.Errorf("%w: password must be at least 8 chars, contain uppercase, lowercase, digit, and special char", ErrValidation)
	}

	hashedPassword, err := s.hasher.HashPassword(req.Password)
	if err != nil {
		return uuid.Nil, err
	}

	arg := repository.CreateUserParams{
		Email:        req.Email,
		PasswordHash: hashedPassword,
		Role:         "user", // Prevent privilege escalation, force role to user
	}
	if req.CustomerID != uuid.Nil {
		arg.CustomerID.Bytes = req.CustomerID
		arg.CustomerID.Valid = true
	}

	user, err := s.repo.CreateUser(ctx, arg)
	if err != nil {
		// Idempotency: if user already exists, return existing ID
		existingUser, errGet := s.repo.GetUserByEmail(ctx, req.Email)
		if errGet == nil {
			return uuid.UUID(existingUser.ID.Bytes), nil
		}
		return uuid.Nil, fmt.Errorf("%w: %v", ErrUserAlreadyExists, err)
	}

	return uuid.UUID(user.ID.Bytes), nil
}

type LoginResponse struct {
	AccessToken  string
	RefreshToken string
	User         repository.User
}

func (s *Service) Login(ctx context.Context, email, password, userAgent, clientIP string, duration time.Duration) (LoginResponse, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	if !emailRegex.MatchString(email) {
		return LoginResponse{}, fmt.Errorf("%w: invalid email format", ErrValidation)
	}

	if s.lockout != nil {
		allowed, err := s.lockout.Allow(ctx, email, 5, 15*time.Minute, 10*time.Minute)
		if err == nil && !allowed {
			return LoginResponse{}, ErrAccountLocked
		}
	}

	var user repository.User
	var userFound bool

	// Get user hash first (outside TX to keep TX short)
	u, err := s.repo.GetUserByEmail(ctx, email)
	var hashToVerify string
	if err == nil {
		hashToVerify = u.PasswordHash
		userFound = true
		user = u
	} else {
		hashToVerify = crypto.DummyHash
		userFound = false
	}

	match, err := crypto.VerifyPassword(password, hashToVerify)

	if !userFound || err != nil || !match {
		s.metrics.InvalidCredentials.Add(1)
		s.metrics.FailedLoginsTotal.Add(1)
		if s.lockout != nil {
			_ = s.lockout.Increment(ctx, email, 5, 15*time.Minute, 10*time.Minute)
		}
		return LoginResponse{}, ErrInvalidCredentials
	}

	if s.lockout != nil {
		_ = s.lockout.Reset(ctx, email)
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

	refreshTokenId, _ := uuid.NewV7()
	refreshTokenStr := uuid.NewString()
	expiresAt := time.Now().Add(7 * 24 * time.Hour)

	err = s.repo.ExecTx(ctx, func(q repository.Querier) error {
		_, err = q.CreateSession(ctx, repository.CreateSessionParams{
			ID:           pgtype.UUID{Bytes: refreshTokenId, Valid: true},
			UserID:       user.ID,
			RefreshToken: refreshTokenStr,
			UserAgent:    userAgent,
			ClientIp:     clientIP,
			IsBlocked:    false,
			ExpiresAt:    pgtype.Timestamptz{Time: expiresAt, Valid: true},
		})
		return err
	})

	if err != nil {
		return LoginResponse{}, fmt.Errorf("failed to create session: %w", err)
	}

	return LoginResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshTokenStr,
		User:         user,
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

func (s *Service) RefreshToken(ctx context.Context, refreshTokenStr string, duration time.Duration) (string, string, error) {
	// Idempotency: check if we recently processed this token
	if s.rdb != nil {
		cached, err := s.rdb.Get(ctx, "idempotency:refresh:"+refreshTokenStr).Result()
		if err == nil && cached != "" {
			parts := strings.Split(cached, " ")
			if len(parts) == 2 {
				return parts[0], parts[1], nil
			}
		}
	}

	var accessToken string
	var newRefreshTokenStr string

	err := s.repo.ExecTx(ctx, func(q repository.Querier) error {
		session, err := q.GetSessionByRefreshTokenForUpdate(ctx, refreshTokenStr)
		if err != nil {
			return fmt.Errorf("invalid refresh token: %w", err)
		}

		if session.IsBlocked {
			return ErrSessionBlocked
		}

		if session.ExpiresAt.Time.Before(time.Now()) {
			return errors.New("refresh token expired")
		}

		user, err := q.GetUserByID(ctx, session.UserID)
		if err != nil {
			return fmt.Errorf("user not found: %w", err)
		}

		// Token rotation: block the old session
		err = q.BlockSession(ctx, session.ID)
		if err != nil {
			return fmt.Errorf("failed to block old session: %w", err)
		}

		accessToken, err = s.tokenMaker.CreateToken(
			uuid.UUID(user.ID.Bytes),
			user.Role,
			uuid.UUID(user.CustomerID.Bytes),
			duration,
		)
		if err != nil {
			return err
		}

		newRefreshTokenId, _ := uuid.NewV7()
		newRefreshTokenStr = uuid.NewString()
		expiresAt := time.Now().Add(7 * 24 * time.Hour)

		_, err = q.CreateSession(ctx, repository.CreateSessionParams{
			ID:           pgtype.UUID{Bytes: newRefreshTokenId, Valid: true},
			UserID:       user.ID,
			RefreshToken: newRefreshTokenStr,
			UserAgent:    session.UserAgent,
			ClientIp:     session.ClientIp,
			IsBlocked:    false,
			ExpiresAt:    pgtype.Timestamptz{Time: expiresAt, Valid: true},
		})
		if err != nil {
			return fmt.Errorf("failed to create new session: %w", err)
		}

		return nil
	})

	if err != nil {
		return "", "", err
	}

	if s.rdb != nil {
		s.rdb.Set(ctx, "idempotency:refresh:"+refreshTokenStr, accessToken+" "+newRefreshTokenStr, 5*time.Minute)
	}

	return accessToken, newRefreshTokenStr, nil
}

func (s *Service) RevokeToken(ctx context.Context, refreshTokenStr string) error {
	return s.repo.BlockSessionByRefreshToken(ctx, refreshTokenStr)
}
