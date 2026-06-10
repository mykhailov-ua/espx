package auth

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"espx/internal/auth/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupIntegrationDB(t testing.TB) (*pgxpool.Pool, func()) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("auth_integration_db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("secure_password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(20*time.Second)),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %s", err)
	}

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %s", err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to connect to db: %s", err)
	}

	_, filename, _, _ := runtime.Caller(0)
	migrationsDir := filepath.Join(filepath.Dir(filename), "migrations")

	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		t.Fatalf("failed to read migrations dir %s: %s", migrationsDir, err)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		sqlBytes, err := os.ReadFile(filepath.Join(migrationsDir, entry.Name()))
		if err != nil {
			t.Fatalf("failed to read migration %s: %s", entry.Name(), err)
		}

		sql := string(sqlBytes)
		parts := strings.Split(sql, "-- +goose Down")
		upPart := parts[0]
		upPart = strings.ReplaceAll(upPart, "-- +goose Up", "")
		upPart = strings.ReplaceAll(upPart, "-- +goose StatementBegin", "")
		upPart = strings.ReplaceAll(upPart, "-- +goose StatementEnd", "")

		if _, err := pool.Exec(ctx, upPart); err != nil {
			t.Fatalf("failed to apply migration %s: %s\nFull SQL attempted:\n%s", entry.Name(), err, upPart)
		}
	}

	return pool, func() {
		pool.Close()
		_ = pgContainer.Terminate(ctx)
	}
}

func setupIntegrationRedis(t testing.TB) (redis.UniversalClient, func()) {
	ctx := context.Background()

	redisContainer, err := rediscontainer.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("failed to start redis container: %s", err)
	}

	endpoint, err := redisContainer.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("failed to get redis endpoint: %s", err)
	}

	rdb := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{endpoint},
	})

	return rdb, func() {
		_ = rdb.Close()
		_ = redisContainer.Terminate(ctx)
	}
}

func TestAuthService_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping testcontainers-based integration test in short mode")
	}

	pool, cleanupDB := setupIntegrationDB(t)
	defer cleanupDB()

	rdb, cleanupRedis := setupIntegrationRedis(t)
	defer cleanupRedis()

	store := db.NewStore(pool)
	tokenMaker, err := NewPasetoMaker("yellow-submarine-yellow-submarin")
	require.NoError(t, err)

	hasher, err := NewPasswordHasher(4096, 1, 1)
	require.NoError(t, err)

	lockout := NewLockoutLimiter(rdb)
	service := NewService(store, tokenMaker, hasher, lockout, rdb)

	ctx := context.Background()
	email := "compliance-officer@company.internal"
	initPassword := "SuperSecure123!"

	userID, err := service.Register(ctx, RegisterDTO{
		Email:    email,
		Password: initPassword,
	})
	require.NoError(t, err)
	assert.NotEqual(t, uuid.Nil, userID)

	history, err := store.GetPasswordHistory(ctx, db.GetPasswordHistoryParams{
		UserID: pgtype.UUID{Bytes: userID, Valid: true},
		Limit:  10,
	})
	require.NoError(t, err)
	require.Len(t, history, 1, "Initial password must be in history to prevent instant cyclic reuse")

	_, err = service.Register(ctx, RegisterDTO{
		Email:    email,
		Password: "AnotherPassword456!",
	})
	assert.ErrorIs(t, err, ErrUserAlreadyExists, "Registration of duplicates must fail neutrally")

	loginResp, err := service.Login(ctx, email, initPassword, "Mozilla/Firefox", "192.168.1.100", time.Hour)
	require.NoError(t, err)
	assert.NotEmpty(t, loginResp.AccessToken)

	err = service.ChangePassword(ctx, userID, initPassword, initPassword, "192.168.1.100", "Mozilla/Firefox")
	assert.ErrorIs(t, err, ErrPasswordReuse, "Password reuse check must reject matching historical hashes")

	newPassword1 := "RotatedPassword456!"
	err = service.ChangePassword(ctx, userID, initPassword, newPassword1, "192.168.1.100", "Mozilla/Firefox")
	require.NoError(t, err, "Valid, non-reused password change should succeed")

	history, err = store.GetPasswordHistory(ctx, db.GetPasswordHistoryParams{
		UserID: pgtype.UUID{Bytes: userID, Valid: true},
		Limit:  10,
	})
	require.NoError(t, err)
	assert.Len(t, history, 2, "Password history should track new password hash")

	newPassword2 := "ThirdExcellentPass789!"
	err = service.ChangePassword(ctx, userID, newPassword1, newPassword2, "192.168.1.100", "Mozilla/Firefox")
	require.NoError(t, err)

	history, err = store.GetPasswordHistory(ctx, db.GetPasswordHistoryParams{
		UserID: pgtype.UUID{Bytes: userID, Valid: true},
		Limit:  10,
	})
	require.NoError(t, err)
	assert.Len(t, history, 3)

	err = service.ChangePassword(ctx, userID, newPassword2, initPassword, "192.168.1.100", "Mozilla/Firefox")
	assert.ErrorIs(t, err, ErrPasswordReuse, "SuperSecure123! is still in the last 3 history and must be blocked")

	token, err := service.RequestEmailVerification(ctx, userID)
	require.NoError(t, err)
	assert.NotEmpty(t, token)

	usr, err := store.GetUserByID(ctx, pgtype.UUID{Bytes: userID, Valid: true})
	require.NoError(t, err)
	assert.False(t, usr.EmailVerified)

	confirmedUID, err := service.ConfirmEmailVerification(ctx, token)
	require.NoError(t, err)
	assert.Equal(t, userID, confirmedUID)

	usr, err = store.GetUserByID(ctx, pgtype.UUID{Bytes: userID, Valid: true})
	require.NoError(t, err)
	assert.True(t, usr.EmailVerified, "Confirming email must persist the verified flag to Postgres")

	_, err = service.ConfirmEmailVerification(ctx, token)
	assert.ErrorIs(t, err, ErrInvalidToken, "Replaying a verification token must be rejected because it was deleted on first use")
}
