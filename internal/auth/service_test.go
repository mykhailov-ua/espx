package auth

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"espx/internal/auth/db"
	"espx/internal/database"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
)

type mockRepo struct {
	db.Querier
	user             db.User
	session          db.Session
	err              error
	createUserErr    error
	getUserByID      db.User
	getUserByIDErr   error
	createSessionErr error
}

func (m *mockRepo) GetUserByEmail(ctx context.Context, email string) (db.User, error) {
	return m.user, m.err
}

func (m *mockRepo) GetUserByID(ctx context.Context, id pgtype.UUID) (db.User, error) {
	return m.getUserByID, m.getUserByIDErr
}

func (m *mockRepo) CreateUser(ctx context.Context, arg db.CreateUserParams) (db.CreateUserRow, error) {
	if m.createUserErr != nil {
		return db.CreateUserRow{}, m.createUserErr
	}
	return db.CreateUserRow{ID: pgtype.UUID{Bytes: uuid.New(), Valid: true}}, nil
}

func (m *mockRepo) CreateSession(ctx context.Context, arg db.CreateSessionParams) (db.Session, error) {
	if m.createSessionErr != nil {
		return db.Session{}, m.createSessionErr
	}
	return m.session, m.err
}

func (m *mockRepo) GetSessionByRefreshTokenForUpdate(ctx context.Context, refreshToken string) (db.Session, error) {
	return m.session, m.err
}

func (m *mockRepo) BlockSession(ctx context.Context, id pgtype.UUID) error {
	return m.err
}

func (m *mockRepo) BlockSessionByRefreshToken(ctx context.Context, refreshToken string) error {
	return m.err
}

func (m *mockRepo) DeleteExpiredOrBlockedSessions(ctx context.Context) (int64, error) {
	return 5, m.err
}

func (m *mockRepo) GetSessionByRefreshToken(ctx context.Context, refreshToken string) (db.Session, error) {
	return m.session, m.err
}

func (m *mockRepo) BlockUser(ctx context.Context, email string) error {
	return m.err
}

func (m *mockRepo) UpdatePassword(ctx context.Context, arg db.UpdatePasswordParams) error {
	return m.err
}

func (m *mockRepo) ExecTx(ctx context.Context, fn func(db.Querier) error) error {
	return fn(m)
}

func (m *mockRepo) CreatePasswordHistoryEntry(ctx context.Context, arg db.CreatePasswordHistoryEntryParams) error {
	return m.err
}

func (m *mockRepo) GetPasswordHistory(ctx context.Context, arg db.GetPasswordHistoryParams) ([]string, error) {
	return nil, m.err
}

func (m *mockRepo) SetEmailVerified(ctx context.Context, id pgtype.UUID) error { return m.err }

func (m *mockRepo) CreateAuthAuditLog(ctx context.Context, arg db.CreateAuthAuditLogParams) (db.CreateAuthAuditLogRow, error) {
	return db.CreateAuthAuditLogRow{}, m.err
}

func (m *mockRepo) ListAuthAuditLogsByUser(ctx context.Context, arg db.ListAuthAuditLogsByUserParams) ([]db.AuthAuditLog, error) {
	return nil, m.err
}

func (m *mockRepo) CreateAPIKey(ctx context.Context, arg db.CreateAPIKeyParams) (db.CreateAPIKeyRow, error) {
	return db.CreateAPIKeyRow{ID: pgtype.UUID{Bytes: uuid.New(), Valid: true}}, nil
}

type mockTokenMaker struct {
	Maker
	createErr error
	verifyErr error
}

func (m *mockTokenMaker) CreateToken(userID uuid.UUID, sessionID uuid.UUID, role string, customerID uuid.UUID, duration time.Duration) (string, error) {
	return "token", m.createErr
}

func (m *mockTokenMaker) VerifyToken(t string) (*Payload, error) {
	return &Payload{UserID: uuid.New()}, m.verifyErr
}

func TestRegister(t *testing.T) {
	repo := &mockRepo{}
	hasher, err := NewPasswordHasher(65536, 3, 4)
	assert.NoError(t, err)
	service := NewService(repo, nil, hasher, nil, nil)

	t.Run("Success", func(t *testing.T) {
		repo.createUserErr = nil
		_, err := service.Register(context.Background(), RegisterDTO{
			Email:    "valid@example.com",
			Password: "Password123!",
		})
		assert.NoError(t, err)
	})

	t.Run("InvalidEmail", func(t *testing.T) {
		_, err := service.Register(context.Background(), RegisterDTO{
			Email:    "invalid",
			Password: "Password123!",
		})
		assert.ErrorIs(t, err, ErrValidation)
	})

	t.Run("InvalidPassword", func(t *testing.T) {
		_, err := service.Register(context.Background(), RegisterDTO{
			Email:    "valid@example.com",
			Password: "short",
		})
		assert.ErrorIs(t, err, ErrValidation)
	})

	t.Run("Idempotency_AlreadyExists", func(t *testing.T) {
		repo.createUserErr = errors.New("unique constraint")
		repo.user = db.User{ID: pgtype.UUID{Bytes: uuid.New(), Valid: true}}
		repo.err = nil
		_, err := service.Register(context.Background(), RegisterDTO{
			Email:    "exists@example.com",
			Password: "Password123!",
		})
		assert.ErrorIs(t, err, ErrUserAlreadyExists)
	})

	t.Run("CreateUserError", func(t *testing.T) {
		repo.createUserErr = errors.New("db error")
		repo.err = errors.New("not found")
		_, err := service.Register(context.Background(), RegisterDTO{
			Email:    "new@example.com",
			Password: "Password123!",
		})
		assert.Error(t, err)
	})
}

func TestLogin(t *testing.T) {
	repo := &mockRepo{}
	tokenMaker := &mockTokenMaker{}
	hasher, err := NewPasswordHasher(65536, 3, 4)
	assert.NoError(t, err)
	service := NewService(repo, tokenMaker, hasher, nil, nil)

	password := "Password123!"
	hash, _ := hasher.HashPassword(password)

	t.Run("Success", func(t *testing.T) {
		repo.user = db.User{
			ID:           pgtype.UUID{Bytes: uuid.New(), Valid: true},
			PasswordHash: hash,
		}
		repo.err = nil
		repo.createSessionErr = nil
		tokenMaker.createErr = nil
		resp, err := service.Login(context.Background(), "user@example.com", password, "ua", "ip", time.Hour)
		assert.NoError(t, err)
		assert.NotEmpty(t, resp.AccessToken)
	})

	t.Run("InvalidEmail", func(t *testing.T) {
		_, err := service.Login(context.Background(), "invalid", "pass", "ua", "ip", time.Hour)
		assert.ErrorIs(t, err, ErrValidation)
	})

	t.Run("InvalidCredentials", func(t *testing.T) {
		repo.user = db.User{PasswordHash: hash}
		_, err := service.Login(context.Background(), "user@example.com", "wrong", "ua", "ip", time.Hour)
		assert.ErrorIs(t, err, ErrInvalidCredentials)
	})

	t.Run("UserNotFound_DummyHash", func(t *testing.T) {
		repo.err = errors.New("not found")
		_, err := service.Login(context.Background(), "unknown@example.com", "password", "ua", "ip", time.Hour)
		assert.ErrorIs(t, err, ErrInvalidCredentials)
	})

	t.Run("TokenMakerError", func(t *testing.T) {
		repo.user = db.User{PasswordHash: hash, ID: pgtype.UUID{Bytes: uuid.New(), Valid: true}}
		repo.err = nil
		tokenMaker.createErr = errors.New("token error")
		_, err := service.Login(context.Background(), "user@example.com", password, "ua", "ip", time.Hour)
		assert.Error(t, err)
	})

	t.Run("CreateSessionError", func(t *testing.T) {
		repo.user = db.User{PasswordHash: hash, ID: pgtype.UUID{Bytes: uuid.New(), Valid: true}}
		repo.err = nil
		tokenMaker.createErr = nil
		repo.createSessionErr = errors.New("session error")
		_, err := service.Login(context.Background(), "user@example.com", password, "ua", "ip", time.Hour)
		assert.Error(t, err)
	})
}

func TestVerifyToken(t *testing.T) {
	repo := &mockRepo{}
	tokenMaker := &mockTokenMaker{}
	service := NewService(repo, tokenMaker, nil, nil, nil)

	t.Run("Success", func(t *testing.T) {
		repo.getUserByID = db.User{Email: "user@example.com"}
		repo.getUserByIDErr = nil
		tokenMaker.verifyErr = nil
		user, err := service.VerifyToken(context.Background(), "valid-token")
		assert.NoError(t, err)
		assert.Equal(t, "user@example.com", user.Email)
	})

	t.Run("InvalidToken", func(t *testing.T) {
		tokenMaker.verifyErr = errors.New("invalid token")
		_, err := service.VerifyToken(context.Background(), "invalid-token")
		assert.Error(t, err)
	})
}

func TestRefreshToken(t *testing.T) {
	repo := &mockRepo{}
	tokenMaker := &mockTokenMaker{}
	service := NewService(repo, tokenMaker, nil, nil, nil)

	t.Run("Success", func(t *testing.T) {
		repo.session = db.Session{
			UserID:    pgtype.UUID{Bytes: uuid.New(), Valid: true},
			ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(time.Hour), Valid: true},
			IsBlocked: false,
		}
		repo.getUserByID = db.User{ID: repo.session.UserID}
		repo.err = nil
		repo.getUserByIDErr = nil
		repo.createSessionErr = nil

		accessToken, refreshToken, err := service.RefreshToken(context.Background(), "old-token", time.Hour)
		assert.NoError(t, err)
		assert.NotEmpty(t, accessToken)
		assert.NotEmpty(t, refreshToken)
	})

	t.Run("BlockedSession", func(t *testing.T) {
		repo.session = db.Session{IsBlocked: true}
		repo.err = nil
		_, _, err := service.RefreshToken(context.Background(), "blocked-token", time.Hour)
		assert.ErrorIs(t, err, ErrSessionBlocked)
	})

	t.Run("TokenExpired", func(t *testing.T) {
		repo.session = db.Session{
			ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(-time.Hour), Valid: true},
			IsBlocked: false,
		}
		_, _, err := service.RefreshToken(context.Background(), "expired-token", time.Hour)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expired")
	})

	t.Run("UserNotFound", func(t *testing.T) {
		repo.session = db.Session{
			ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(time.Hour), Valid: true},
		}
		repo.getUserByIDErr = errors.New("user not found")
		_, _, err := service.RefreshToken(context.Background(), "token", time.Hour)
		assert.Error(t, err)
	})

	t.Run("TokenMakerError", func(t *testing.T) {
		repo.session = db.Session{
			UserID:    pgtype.UUID{Bytes: uuid.New(), Valid: true},
			ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(time.Hour), Valid: true},
		}
		repo.getUserByID = db.User{ID: repo.session.UserID}
		tokenMaker.createErr = errors.New("token error")
		_, _, err := service.RefreshToken(context.Background(), "token", time.Hour)
		assert.Error(t, err)
	})

	t.Run("CreateSessionError", func(t *testing.T) {
		repo.session = db.Session{
			UserID:    pgtype.UUID{Bytes: uuid.New(), Valid: true},
			ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(time.Hour), Valid: true},
		}
		repo.getUserByID = db.User{ID: repo.session.UserID}
		tokenMaker.createErr = nil
		repo.createSessionErr = errors.New("session error")
		_, _, err := service.RefreshToken(context.Background(), "token", time.Hour)
		assert.Error(t, err)
	})
}

func TestRevokeToken(t *testing.T) {
	repo := &mockRepo{}
	service := NewService(repo, nil, nil, nil, nil)

	repo.err = nil
	err := service.RevokeToken(context.Background(), "token-to-revoke")
	assert.NoError(t, err)
}

func TestSessionCleanupWorker(t *testing.T) {
	repo := &mockRepo{}
	service := NewService(repo, nil, nil, nil, nil)
	worker := NewSessionCleanupWorker(service)

	err := worker.Cleanup(context.Background())
	assert.NoError(t, err)
}

func TestLoginFlood(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()

	hasher, err := NewPasswordHasher(65536, 3, 4)
	assert.NoError(t, err)
	repo := &mockRepo{
		user: db.User{
			ID:           pgtype.UUID{Bytes: uuid.New(), Valid: true},
			PasswordHash: hasher.GetDummyHash(),
		},
	}
	lockout := NewLockoutLimiter(rdb)
	service := NewService(repo, nil, hasher, lockout, rdb)

	email := "flood@example.com"
	clientIP := "test-ip"
	var wg sync.WaitGroup
	var lockedCount atomic.Int32
	var rateLimitedCount atomic.Int32

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := service.Login(context.Background(), email, "wrong-password", "ua", clientIP, time.Hour)
			if errors.Is(err, ErrAccountLocked) {
				lockedCount.Add(1)
			} else if errors.Is(err, ErrRateLimitExceeded) {
				rateLimitedCount.Add(1)
			}
		}()
	}
	wg.Wait()

	assert.GreaterOrEqual(t, int(rateLimitedCount.Load()), 25)
	assert.GreaterOrEqual(t, int(lockedCount.Load()), 10)
}
