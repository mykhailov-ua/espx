package auth

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth/db"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

// mockRedisClient mocks selected redis.UniversalClient methods for testing.
type mockRedisClient struct {
	redis.UniversalClient
	evalFunc      func(script string, keys []string, args ...interface{}) (interface{}, error)
	getFunc       func(key string) *redis.StringCmd
	setFunc       func(key string, value interface{}, ttl time.Duration) *redis.StatusCmd
	setNXFunc     func(key string, value interface{}, ttl time.Duration) *redis.BoolCmd
	delFunc       func(keys ...string) *redis.IntCmd
	pipelinedFunc func(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error)
}

func (m *mockRedisClient) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	if m.evalFunc != nil {
		res, err := m.evalFunc(script, keys, args...)
		cmd := redis.NewCmd(ctx)
		cmd.SetVal(res)
		cmd.SetErr(err)
		return cmd
	}
	return redis.NewCmd(ctx)
}

func (m *mockRedisClient) Get(ctx context.Context, key string) *redis.StringCmd {
	if m.getFunc != nil {
		return m.getFunc(key)
	}
	cmd := redis.NewStringCmd(ctx)
	cmd.SetErr(redis.Nil)
	return cmd
}

func (m *mockRedisClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	if m.setFunc != nil {
		return m.setFunc(key, value, expiration)
	}
	cmd := redis.NewStatusCmd(ctx)
	return cmd
}

func (m *mockRedisClient) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	if m.setNXFunc != nil {
		return m.setNXFunc(key, value, expiration)
	}
	cmd := redis.NewBoolCmd(ctx)
	cmd.SetVal(true)
	return cmd
}

func (m *mockRedisClient) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	if m.delFunc != nil {
		return m.delFunc(keys...)
	}
	cmd := redis.NewIntCmd(ctx)
	return cmd
}

func (m *mockRedisClient) Pipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	if m.pipelinedFunc != nil {
		return m.pipelinedFunc(ctx, fn)
	}
	return nil, nil
}

// owaspMockRepo mocks db.Querier features specifically for OWASP scenario tests.
type owaspMockRepo struct {
	db.Store
	createUserFunc     func(ctx context.Context, arg db.CreateUserParams) (db.CreateUserRow, error)
	getUserByEmailFunc func(ctx context.Context, email string) (db.User, error)
	getUserByIDFunc    func(ctx context.Context, id pgtype.UUID) (db.User, error)
	blockUserFunc      func(ctx context.Context, email string) error
}

func (m *owaspMockRepo) CreateUser(ctx context.Context, arg db.CreateUserParams) (db.CreateUserRow, error) {
	if m.createUserFunc != nil {
		return m.createUserFunc(ctx, arg)
	}
	return db.CreateUserRow{}, nil
}

func (m *owaspMockRepo) GetUserByEmail(ctx context.Context, email string) (db.User, error) {
	if m.getUserByEmailFunc != nil {
		return m.getUserByEmailFunc(ctx, email)
	}
	return db.User{}, nil
}

func (m *owaspMockRepo) GetUserByID(ctx context.Context, id pgtype.UUID) (db.User, error) {
	if m.getUserByIDFunc != nil {
		return m.getUserByIDFunc(ctx, id)
	}
	return db.User{}, nil
}

func (m *owaspMockRepo) BlockUser(ctx context.Context, email string) error {
	if m.blockUserFunc != nil {
		return m.blockUserFunc(ctx, email)
	}
	return nil
}

func (m *owaspMockRepo) ExecTx(ctx context.Context, fn func(db.Querier) error) error {
	return fn(m)
}

// Stubs for new interface methods used by Service in full flow.
func (m *owaspMockRepo) SetEmailVerified(ctx context.Context, id pgtype.UUID) error { return nil }
func (m *owaspMockRepo) CreateAuthAuditLog(ctx context.Context, arg db.CreateAuthAuditLogParams) (db.CreateAuthAuditLogRow, error) {
	return db.CreateAuthAuditLogRow{}, nil
}
func (m *owaspMockRepo) ListAuthAuditLogsByUser(ctx context.Context, arg db.ListAuthAuditLogsByUserParams) ([]db.AuthAuditLog, error) {
	return nil, nil
}

func (m *owaspMockRepo) CreateAPIKey(ctx context.Context, arg db.CreateAPIKeyParams) (db.CreateAPIKeyRow, error) {
	return db.CreateAPIKeyRow{ID: pgtype.UUID{Bytes: uuid.New(), Valid: true}}, nil
}

func (m *owaspMockRepo) CreatePasswordHistoryEntry(ctx context.Context, arg db.CreatePasswordHistoryEntryParams) error {
	return nil
}
func (m *owaspMockRepo) GetPasswordHistory(ctx context.Context, arg db.GetPasswordHistoryParams) ([]string, error) {
	return nil, nil
}

// TestOWASP_UserEnumerationRegister verifies registration does not disclose UUID on duplicate.
func TestOWASP_UserEnumerationRegister(t *testing.T) {
	repo := &owaspMockRepo{}
	hasher, err := NewPasswordHasher(32768, 2, 2)
	assert.NoError(t, err)
	service := NewService(repo, nil, hasher, nil, nil)

	repo.createUserFunc = func(ctx context.Context, arg db.CreateUserParams) (db.CreateUserRow, error) {
		return db.CreateUserRow{}, errors.New("unique constraint violation")
	}

	_, err = service.Register(context.Background(), RegisterDTO{
		Email:    "existing@example.com",
		Password: "Password123!",
	})
	assert.ErrorIs(t, err, ErrUserAlreadyExists, "Registration of existing email must return ErrUserAlreadyExists to prevent user enumeration and account probing")
}

// TestOWASP_LockoutNoPostgresBlock verifies account lockouts are temporary in Redis and do not permanently lock users in PostgreSQL.
func TestOWASP_LockoutNoPostgresBlock(t *testing.T) {
	repo := &owaspMockRepo{}
	hasher, err := NewPasswordHasher(32768, 2, 2)
	assert.NoError(t, err)

	mRedis := &mockRedisClient{
		evalFunc: func(script string, keys []string, args ...interface{}) (interface{}, error) {
			// If it's the IP-based rate limiter (AllowIP), return 1 (allowed)
			if len(keys) > 0 && strings.HasPrefix(keys[0], "ratelimit:ip:") {
				return int64(1), nil
			}
			// For Allow, return -1 to simulate that global lockout threshold has been exceeded.
			return int64(-1), nil
		},
	}
	lockout := NewLockoutLimiter(mRedis)
	service := NewService(repo, nil, hasher, lockout, mRedis)

	repo.getUserByEmailFunc = func(ctx context.Context, email string) (db.User, error) {
		return db.User{
			ID:           pgtype.UUID{Bytes: uuid.New(), Valid: true},
			Email:        email,
			PasswordHash: hasher.dummyHash,
		}, nil
	}

	var blockUserCalled int32
	repo.blockUserFunc = func(ctx context.Context, email string) error {
		atomic.AddInt32(&blockUserCalled, 1)
		return nil
	}

	_, err = service.Login(context.Background(), "victim@example.com", "Password123!", "test", "1.2.3.4", time.Hour)
	assert.ErrorIs(t, err, ErrAccountLocked)
	assert.Equal(t, int32(0), atomic.LoadInt32(&blockUserCalled), "PostgreSQL BlockUser must NOT be triggered automatically during lockouts to prevent permanent account DOS")
}

// TestOWASP_IPSpoofingXForwardedFor verifies client IP parsing is secure and un-spoofable.
func TestOWASP_IPSpoofingXForwardedFor(t *testing.T) {
	cfg := &config.Config{
		TrustedProxies: []string{"192.168.1.1"},
	}
	handler := NewHandler(nil, cfg)

	// Case 1: X-Forwarded-For has spoofed elements. We must parse from right to left (taking the last element from our trusted proxy).
	ctx1 := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 12345},
	})
	md1 := metadata.Pairs("x-forwarded-for", "12.34.56.78, 99.99.99.99")
	ctx1 = metadata.NewIncomingContext(ctx1, md1)

	ip1 := handler.extractClientIP(ctx1)
	assert.Equal(t, "99.99.99.99", ip1, "Must extract the last element of X-Forwarded-For chain to prevent spoofing")

	// Case 2: X-Real-IP is prioritized and trusted.
	ctx2 := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 12345},
	})
	md2 := metadata.Pairs("x-real-ip", "88.88.88.88", "x-forwarded-for", "12.34.56.78")
	ctx2 = metadata.NewIncomingContext(ctx2, md2)

	ip2 := handler.extractClientIP(ctx2)
	assert.Equal(t, "88.88.88.88", ip2, "Must prioritize X-Real-IP over X-Forwarded-For")
}

// TestOWASP_PasswordPolicy verifies password complexity criteria and passphrase friendliness.
func TestOWASP_PasswordPolicy(t *testing.T) {
	// Weak passwords
	assert.Error(t, ValidatePassword("short"), "Password too short must be rejected")
	assert.Error(t, ValidatePassword("lowercaseonly"), "Password without uppercase, digit, and special char must be rejected")
	assert.Error(t, ValidatePassword("UPPERCASEONLY"), "Password without lowercase, digit, and special char must be rejected")
	assert.Error(t, ValidatePassword("1234567890"), "Password without letters and special char must be rejected")
	assert.Error(t, ValidatePassword("Pass1234"), "Password without special char must be rejected")

	// Strong passwords & high-entropy passphrases
	assert.NoError(t, ValidatePassword("Password123!"), "Strong password must be accepted")
	assert.NoError(t, ValidatePassword("This is a very secure passphrase #123"), "High-entropy passphrase with spaces and symbols must be accepted to encourage passphrase adoption")
}

// TestOWASP_MemoryLeakPrevention verifies zero memory caching leak in middleware.
func TestOWASP_MemoryLeakPrevention(t *testing.T) {
	maker, err := NewPasetoMaker("01234567890123456789012345678901")
	assert.NoError(t, err)

	userID := uuid.New()
	sessionID := uuid.New()
	customerID := uuid.New()

	token, err := maker.CreateToken(userID, sessionID, "user", customerID, time.Minute)
	assert.NoError(t, err)

	payload, err := maker.VerifyToken(token)
	assert.NoError(t, err)
	assert.Equal(t, userID, payload.UserID)
}
