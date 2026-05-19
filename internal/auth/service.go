package auth

import (
	"context"
	"errors"
	"fmt"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth/pb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"regexp"
	"strings"
	"time"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth/db"
	"github.com/redis/go-redis/v9"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ErrUserAlreadyExists  = errors.New("user already exists")
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrAccountLocked      = errors.New("account locked due to too many failed attempts")
	ErrRateLimitExceeded  = errors.New("rate limit exceeded")
	ErrValidation         = errors.New("validation failed")
	ErrSessionBlocked     = errors.New("session is blocked")
)

var (
	emailRegex    = regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
	passwordRegex = regexp.MustCompile(`^[A-Za-z\d@$!%*?&]{8,}$`)
)

var (
	AuthLoginAttempts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "auth_login_attempts_total",
			Help: "Total number of login attempts",
		},
		[]string{"status", "failure_reason"},
	)
	AuthTokenErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "auth_token_validation_errors_total",
			Help: "Total number of token validation errors",
		},
		[]string{"error_type"},
	)
)

func init() {
	prometheus.MustRegister(AuthLoginAttempts)
	prometheus.MustRegister(AuthTokenErrors)
}

type Service struct {
	repo       db.Store
	tokenMaker Maker
	hasher     *PasswordHasher
	lockout    *LockoutLimiter
	rdb        redis.UniversalClient
	rehashSem  chan struct{}
}

func NewService(repo db.Store, tokenMaker Maker, hasher *PasswordHasher, lockout *LockoutLimiter, rdb redis.UniversalClient) *Service {
	return &Service{
		repo:       repo,
		tokenMaker: tokenMaker,
		hasher:     hasher,
		lockout:    lockout,
		rdb:        rdb,
		rehashSem:  make(chan struct{}, 2), // limit concurrent background Argon2id rehashing to at most 2 to avoid CPU exhaustion
	}
}

type RegisterDTO struct {
	Email      string
	Password   string
	Role       string
	CustomerID uuid.UUID
}

func (s *Service) Register(ctx context.Context, req RegisterDTO) (uuid.UUID, error) {
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

	arg := db.CreateUserParams{
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
		// User creation deduplication avoids race-induced duplicate registration errors under concurrent requests.
		existingUser, errGet := s.repo.GetUserByEmail(ctx, req.Email)
		if errGet == nil {
			return uuid.UUID(existingUser.ID.Bytes), nil
		}
		return uuid.Nil, fmt.Errorf("%w: %v", ErrUserAlreadyExists, err)
	}

	return uuid.UUID(user.ID.Bytes), nil
}

type LoginDTO struct {
	AccessToken  string
	RefreshToken string
	User         db.User
}

func (s *Service) Login(ctx context.Context, email, password, userAgent, clientIP string, duration time.Duration) (pb.LoginResponse, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	if !emailRegex.MatchString(email) {
		return pb.LoginResponse{}, fmt.Errorf("%w: invalid email format", ErrValidation)
	}

	if s.lockout != nil {
		allowedIP, errIP := s.lockout.AllowIP(ctx, clientIP, 20, time.Minute)
		if errIP == nil && !allowedIP {
			AuthLoginAttempts.WithLabelValues("failure", "ratelimit").Inc()
			slog.Warn("security_audit_event", slog.String("event", "auth_failure"), slog.String("ip", clientIP), slog.String("email", email), slog.String("reason", "ip rate limit exceeded"))
			return pb.LoginResponse{}, ErrRateLimitExceeded
		}

		allowed, err := s.lockout.Allow(ctx, clientIP, email, 5, 15*time.Minute, 10*time.Minute)
		if err == nil {
			if allowed == 0 {
				AuthLoginAttempts.WithLabelValues("failure", "locked").Inc()
				slog.Warn("security_audit_event", slog.String("event", "auth_failure"), slog.String("ip", clientIP), slog.String("email", email), slog.String("reason", "account locked by ip"))
				return pb.LoginResponse{}, ErrAccountLocked
			} else if allowed == -1 {
				AuthLoginAttempts.WithLabelValues("failure", "global_locked").Inc()
				slog.Warn("security_audit_event", slog.String("event", "auth_failure"), slog.String("ip", clientIP), slog.String("email", email), slog.String("reason", "global account lockout triggered"))
				_ = s.repo.ExecTx(ctx, func(q db.Querier) error {
					return q.BlockUser(ctx, email)
				})
				return pb.LoginResponse{}, ErrAccountLocked
			}
		}
		defer func() {
			_ = s.lockout.DecrementInflight(ctx, clientIP, email)
		}()
	}

	var user db.User
	var userFound bool

	// Pre-retrieval of user records outside the main txn minimizes locks on the hot path.
	// We verify credentials with a dynamic dummy hash on miss to preserve uniform time profiles against timing attacks.
	u, err := s.repo.GetUserByEmail(ctx, email)
	var hashToVerify string
	if err == nil {
		hashToVerify = u.PasswordHash
		userFound = true
		user = u
	} else {
		hashToVerify = s.hasher.GetDummyHash()
		userFound = false
	}

	match, verifyErr := VerifyPassword(password, hashToVerify)

	if !userFound || (verifyErr != nil && !errors.Is(verifyErr, ErrInsecureHashParameters)) || !match {
		AuthLoginAttempts.WithLabelValues("failure", "invalid_credentials").Inc()
		slog.Warn("security_audit_event", slog.String("event", "auth_failure"), slog.String("ip", clientIP), slog.String("email", email), slog.String("reason", "invalid credentials"))
		if s.lockout != nil {
			res, _ := s.lockout.Increment(ctx, clientIP, email, 5, 15*time.Minute, 10*time.Minute)
			if res == -1 {
				_ = s.repo.ExecTx(ctx, func(q db.Querier) error {
					return q.BlockUser(ctx, email)
				})
			}
		}
		return pb.LoginResponse{}, ErrInvalidCredentials
	}

	// Background migration to Argon2id occurs only if the password hash has insecure parameters.
	// We throttle concurrent executions using a local semaphore and distributed lock to prevent CPU exhaustion DoS.
	if errors.Is(verifyErr, ErrInsecureHashParameters) {
		lockKey := "lock:rehash:" + email
		if ok, _ := s.rdb.SetNX(ctx, lockKey, "1", time.Minute).Result(); ok {
			select {
			case s.rehashSem <- struct{}{}:
				go func(plainPwd, userEmail string) {
					defer func() {
						<-s.rehashSem
						s.rdb.Del(context.Background(), lockKey)
					}()
					newHash, errHash := s.hasher.HashPassword(plainPwd)
					if errHash == nil {
						_ = s.repo.UpdatePassword(context.Background(), db.UpdatePasswordParams{
							Email:        userEmail,
							PasswordHash: newHash,
						})
					}
				}(password, email)
			default:
				s.rdb.Del(ctx, lockKey)
			}
		}
	}

	if s.lockout != nil {
		_ = s.lockout.Reset(ctx, clientIP, email)
	}

	refreshTokenId, _ := uuid.NewV7()

	accessToken, err := s.tokenMaker.CreateToken(
		uuid.UUID(user.ID.Bytes),
		refreshTokenId,
		user.Role,
		uuid.UUID(user.CustomerID.Bytes),
		duration,
	)
	if err != nil {
		AuthLoginAttempts.WithLabelValues("failure", "error").Inc()
		return pb.LoginResponse{}, err
	}

	AuthLoginAttempts.WithLabelValues("success", "").Inc()

	refreshTokenStr := uuid.NewString()
	expiresAt := time.Now().Add(7 * 24 * time.Hour)

	err = s.repo.ExecTx(ctx, func(q db.Querier) error {
		_, err = q.CreateSession(ctx, db.CreateSessionParams{
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
		return pb.LoginResponse{}, fmt.Errorf("failed to create session: %w", err)
	}

	return pb.LoginResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshTokenStr,
		User: &pb.User{
			ID:         uuid.UUID(user.ID.Bytes).String(),
			Email:      user.Email,
			Role:       user.Role,
			CustomerID: uuid.UUID(user.CustomerID.Bytes).String(),
			CreatedAt:  timestamppb.New(user.CreatedAt.Time),
		},
	}, nil
}

func (s *Service) VerifyToken(ctx context.Context, accessToken string) (db.User, error) {
	payload, err := s.tokenMaker.VerifyToken(accessToken)
	if err != nil {
		AuthTokenErrors.WithLabelValues("invalid").Inc()
		return db.User{}, err
	}

	if s.rdb != nil {
		ctxRevoked, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		cmds, errPipe := s.rdb.Pipelined(ctxRevoked, func(pipe redis.Pipeliner) error {
			pipe.Exists(ctxRevoked, "revoked:token:"+payload.ID.String())
			pipe.Exists(ctxRevoked, "revoked:session:"+payload.SessionID.String())
			pipe.Exists(ctxRevoked, "revoked:user:"+payload.UserID.String())
			return nil
		})
		
		if errPipe == nil && len(cmds) == 3 {
			for _, cmd := range cmds {
				if exists, _ := cmd.(*redis.IntCmd).Result(); exists > 0 {
					return db.User{}, ErrSessionBlocked
				}
			}
		}
	}

	user, err := s.repo.GetUserByID(ctx, pgtype.UUID{Bytes: payload.UserID, Valid: true})
	if err != nil {
		return db.User{}, err
	}

	if user.IsBlocked {
		return db.User{}, ErrSessionBlocked
	}

	return user, nil
}

func (s *Service) RefreshToken(ctx context.Context, refreshTokenStr string, duration time.Duration) (string, string, error) {
	// Cached token results absorb retry-storms from network jitter.
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

	// Serializable transactions isolate session rotation to prevent race-induced split-brain duplicate sessions.
	err := s.repo.ExecTx(ctx, func(q db.Querier) error {
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

		if user.IsBlocked {
			return ErrSessionBlocked
		}

		err = q.BlockSession(ctx, session.ID)
		if err != nil {
			return fmt.Errorf("failed to block old session: %w", err)
		}

		newRefreshTokenId, _ := uuid.NewV7()

		accessToken, err = s.tokenMaker.CreateToken(
			uuid.UUID(user.ID.Bytes),
			newRefreshTokenId,
			user.Role,
			uuid.UUID(user.CustomerID.Bytes),
			duration,
		)
		if err != nil {
			return err
		}

		newRefreshTokenStr = uuid.NewString()
		expiresAt := time.Now().Add(7 * 24 * time.Hour)

		_, err = q.CreateSession(ctx, db.CreateSessionParams{
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
	session, err := s.repo.GetSessionByRefreshToken(ctx, refreshTokenStr)
	if err == nil && s.rdb != nil {
		sessionID := uuid.UUID(session.ID.Bytes).String()
		s.rdb.Set(ctx, "revoked:session:"+sessionID, "1", 24*time.Hour)
	}
	return s.repo.BlockSessionByRefreshToken(ctx, refreshTokenStr)
}
