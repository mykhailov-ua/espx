package auth

import (
	"context"
	"errors"
	"fmt"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth/pb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"regexp"
	"runtime"
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

type idempotentResultError struct {
	accessToken  string
	refreshToken string
}

func (e *idempotentResultError) Error() string {
	return "idempotency hit"
}

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
	cryptoSem  chan struct{}
}

func NewService(repo db.Store, tokenMaker Maker, hasher *PasswordHasher, lockout *LockoutLimiter, rdb redis.UniversalClient) *Service {
	gomaxprocs := runtime.GOMAXPROCS(0)
	cryptoLimit := gomaxprocs - 1
	if cryptoLimit < 1 {
		cryptoLimit = 1
	}

	return &Service{
		repo:       repo,
		tokenMaker: tokenMaker,
		hasher:     hasher,
		lockout:    lockout,
		rdb:        rdb,
		rehashSem:  make(chan struct{}, 2),
		cryptoSem:  make(chan struct{}, cryptoLimit),
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
		Role:         "user",
	}
	if req.CustomerID != uuid.Nil {
		arg.CustomerID.Bytes = req.CustomerID
		arg.CustomerID.Valid = true
	}

	user, err := s.repo.CreateUser(ctx, arg)
	if err != nil {
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
				if errTx := s.repo.ExecTx(ctx, func(q db.Querier) error {
					return q.BlockUser(ctx, email)
				}); errTx != nil {
					slog.Error("failed to block user after lockout", slog.String("email", email), slog.Any("error", errTx))
				}
				return pb.LoginResponse{}, ErrAccountLocked
			}
		}
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			if errDec := s.lockout.DecrementInflight(cleanupCtx, clientIP, email); errDec != nil {
				slog.Error("failed to decrement inflight count", slog.String("ip", clientIP), slog.String("email", email), slog.Any("error", errDec))
			}
			cancel()
		}()
	}

	var user db.User
	var userFound bool

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

	select {
	case s.cryptoSem <- struct{}{}:
	case <-ctx.Done():
		return pb.LoginResponse{}, ctx.Err()
	default:
		AuthLoginAttempts.WithLabelValues("failure", "ratelimit").Inc()
		slog.Warn("security_audit_event", slog.String("event", "auth_failure"), slog.String("ip", clientIP), slog.String("email", email), slog.String("reason", "server crypto load limit exceeded"))
		return pb.LoginResponse{}, ErrRateLimitExceeded
	}
	defer func() { <-s.cryptoSem }()

	match, verifyErr := VerifyPassword(password, hashToVerify)

	if !userFound || (verifyErr != nil && !errors.Is(verifyErr, ErrInsecureHashParameters)) || !match {
		AuthLoginAttempts.WithLabelValues("failure", "invalid_credentials").Inc()
		slog.Warn("security_audit_event", slog.String("event", "auth_failure"), slog.String("ip", clientIP), slog.String("email", email), slog.String("reason", "invalid credentials"))
		if s.lockout != nil {
			res, errInc := s.lockout.Increment(ctx, clientIP, email, 5, 15*time.Minute, 10*time.Minute)
			if errInc != nil {
				slog.Error("failed to increment lockout count", slog.String("ip", clientIP), slog.String("email", email), slog.Any("error", errInc))
			} else if res == -1 {
				if errTx := s.repo.ExecTx(ctx, func(q db.Querier) error {
					return q.BlockUser(ctx, email)
				}); errTx != nil {
					slog.Error("failed to block user after lockout increment", slog.String("email", email), slog.Any("error", errTx))
				}
			}
		}
		return pb.LoginResponse{}, ErrInvalidCredentials
	}

	if errors.Is(verifyErr, ErrInsecureHashParameters) {
		lockKey := "lock:rehash:" + email
		ok, errLock := s.rdb.SetNX(ctx, lockKey, "1", time.Minute).Result()
		if errLock != nil {
			slog.Error("failed to acquire rehash lock", slog.String("email", email), slog.Any("error", errLock))
		}
		if ok {
			select {
			case s.rehashSem <- struct{}{}:
				go func(plainPwd, userEmail string) {
					defer func() {
						<-s.rehashSem
						cleanupCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
						if errDel := s.rdb.Del(cleanupCtx, lockKey).Err(); errDel != nil {
							slog.Error("failed to release rehash lock", slog.String("email", userEmail), slog.Any("error", errDel))
						}
						cancel()
					}()
					newHash, errHash := s.hasher.HashPassword(plainPwd)
					if errHash != nil {
						slog.Error("failed to hash password during rehash", slog.String("email", userEmail), slog.Any("error", errHash))
						return
					}
					if errUpd := s.repo.UpdatePassword(context.Background(), db.UpdatePasswordParams{
						Email:        userEmail,
						PasswordHash: newHash,
					}); errUpd != nil {
						slog.Error("failed to update rehashed password", slog.String("email", userEmail), slog.Any("error", errUpd))
					}
				}(password, email)
			default:
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				if errDel := s.rdb.Del(cleanupCtx, lockKey).Err(); errDel != nil {
					slog.Error("failed to release rehash lock on default", slog.String("email", email), slog.Any("error", errDel))
				}
				cancel()
			}
		}
	}

	if s.lockout != nil {
		if errReset := s.lockout.Reset(ctx, clientIP, email); errReset != nil {
			slog.Error("failed to reset lockout status", slog.String("ip", clientIP), slog.String("email", email), slog.Any("error", errReset))
		}
	}

	refreshTokenId := uuid.Must(uuid.NewV7())

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
		if _, err = q.CreateSession(ctx, db.CreateSessionParams{
			ID:           pgtype.UUID{Bytes: refreshTokenId, Valid: true},
			UserID:       user.ID,
			RefreshToken: refreshTokenStr,
			UserAgent:    userAgent,
			ClientIp:     clientIP,
			IsBlocked:    false,
			ExpiresAt:    pgtype.Timestamptz{Time: expiresAt, Valid: true},
		}); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return pb.LoginResponse{}, fmt.Errorf("failed to create session: %w", err)
	}

	return pb.LoginResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshTokenStr,
		User: &pb.User{
			Id:         uuid.UUID(user.ID.Bytes).String(),
			Email:      user.Email,
			Role:       user.Role,
			CustomerId: uuid.UUID(user.CustomerID.Bytes).String(),
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
		cmds, errPipe := s.rdb.Pipelined(ctxRevoked, func(pipe redis.Pipeliner) error {
			pipe.Exists(ctxRevoked, "revoked:token:"+payload.ID.String())
			pipe.Exists(ctxRevoked, "revoked:session:"+payload.SessionID.String())
			pipe.Exists(ctxRevoked, "revoked:user:"+payload.UserID.String())
			return nil
		})
		cancel()
		
		if errPipe != nil {
			AuthTokenErrors.WithLabelValues("revocation_check_failed").Inc()
			slog.Error("failed to check token revocation in redis (fail-closed)", slog.Any("error", errPipe))
			return db.User{}, ErrSessionBlocked
		}
		if len(cmds) != 3 {
			AuthTokenErrors.WithLabelValues("revocation_check_failed").Inc()
			slog.Error("unexpected pipeline commands count in redis (fail-closed)", slog.Int("expected", 3), slog.Int("got", len(cmds)))
			return db.User{}, ErrSessionBlocked
		}

		for _, cmd := range cmds {
			intCmd, ok := cmd.(*redis.IntCmd)
			if !ok {
				AuthTokenErrors.WithLabelValues("revocation_check_failed").Inc()
				slog.Error("unexpected command type in redis pipeline (fail-closed)")
				return db.User{}, ErrSessionBlocked
			}
			exists, errExists := intCmd.Result()
			if errExists != nil {
				AuthTokenErrors.WithLabelValues("revocation_check_failed").Inc()
				slog.Error("failed to get pipeline result in redis (fail-closed)", slog.Any("error", errExists))
				return db.User{}, ErrSessionBlocked
			}
			if exists > 0 {
				return db.User{}, ErrSessionBlocked
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

	err := s.repo.ExecTx(ctx, func(q db.Querier) error {
		session, err := q.GetSessionByRefreshTokenForUpdate(ctx, refreshTokenStr)
		if err != nil {
			return fmt.Errorf("invalid refresh token: %w", err)
		}

		if session.IsBlocked {
			if s.rdb != nil {
				cached, errCached := s.rdb.Get(ctx, "idempotency:refresh:"+refreshTokenStr).Result()
				if errCached == nil && cached != "" {
					parts := strings.Split(cached, " ")
					if len(parts) == 2 {
						return &idempotentResultError{
							accessToken:  parts[0],
							refreshToken: parts[1],
						}
					}
				}
			}
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

		newRefreshTokenId := uuid.Must(uuid.NewV7())

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

		if _, err = q.CreateSession(ctx, db.CreateSessionParams{
			ID:           pgtype.UUID{Bytes: newRefreshTokenId, Valid: true},
			UserID:       user.ID,
			RefreshToken: newRefreshTokenStr,
			UserAgent:    session.UserAgent,
			ClientIp:     session.ClientIp,
			IsBlocked:    false,
			ExpiresAt:    pgtype.Timestamptz{Time: expiresAt, Valid: true},
		}); err != nil {
			return fmt.Errorf("failed to create new session: %w", err)
		}

		return nil
	})

	if err != nil {
		var idmpErr *idempotentResultError
		if errors.As(err, &idmpErr) {
			return idmpErr.accessToken, idmpErr.refreshToken, nil
		}
		return "", "", err
	}

	if s.rdb != nil {
		if errSet := s.rdb.Set(ctx, "idempotency:refresh:"+refreshTokenStr, accessToken+" "+newRefreshTokenStr, 5*time.Minute).Err(); errSet != nil {
			slog.Error("failed to set idempotency cache", slog.Any("error", errSet))
		}
	}

	return accessToken, newRefreshTokenStr, nil
}

func (s *Service) RevokeToken(ctx context.Context, refreshTokenStr string) error {
	session, err := s.repo.GetSessionByRefreshToken(ctx, refreshTokenStr)
	if err == nil && s.rdb != nil {
		sessionID := uuid.UUID(session.ID.Bytes).String()
		if errSet := s.rdb.Set(ctx, "revoked:session:"+sessionID, "1", 24*time.Hour).Err(); errSet != nil {
			slog.Error("failed to set revoked session in redis", slog.String("session_id", sessionID), slog.Any("error", errSet))
		}
	}
	return s.repo.BlockSessionByRefreshToken(ctx, refreshTokenStr)
}
