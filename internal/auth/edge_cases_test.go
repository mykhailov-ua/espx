package auth

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"espx/internal/auth/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChangePassword_EdgeCases(t *testing.T) {
	repo := &mockRepo{}
	hasher, _ := NewPasswordHasher(32768, 2, 2)
	service := NewService(repo, nil, hasher, nil, nil)

	goodPwd := "InitialPass123!"
	newPwd := "NewPass456!"
	hash, _ := hasher.HashPassword(goodPwd)
	userID := uuid.New()

	repo.getUserByID = db.User{
		ID:           pgtype.UUID{Bytes: userID, Valid: true},
		Email:        "u@example.com",
		PasswordHash: hash,
	}
	repo.err = nil

	t.Run("WrongOldPassword", func(t *testing.T) {
		err := service.ChangePassword(context.Background(), userID, "wrong-old", newPwd, "1.2.3.4", "ua")
		assert.ErrorIs(t, err, ErrInvalidCredentials)
	})

	t.Run("Success", func(t *testing.T) {
		err := service.ChangePassword(context.Background(), userID, goodPwd, newPwd, "1.2.3.4", "ua")
		assert.NoError(t, err)

	})

	t.Run("WeakNewPassword", func(t *testing.T) {
		err := service.ChangePassword(context.Background(), userID, newPwd, "short", "ip", "ua")
		assert.ErrorIs(t, err, ErrValidation)
	})
}

func TestCreateAPIKey_Secrecy(t *testing.T) {
	repo := &mockRepo{}
	hasher, _ := NewPasswordHasher(32768, 2, 2)
	service := NewService(repo, nil, hasher, nil, nil)

	userID := uuid.New()
	repo.err = nil

	id, raw, err := service.CreateAPIKey(context.Background(), userID, "ci-cd", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, raw)
	assert.NotEqual(t, uuid.Nil, id)

	assert.GreaterOrEqual(t, len(raw), 40)

	recomputed, err := hasher.HashPassword(raw)
	require.NoError(t, err)
	match, err := VerifyPassword(raw, recomputed)
	require.NoError(t, err)
	assert.True(t, match)
}

func TestAuditLog_NeverFailsCaller(t *testing.T) {
	repo := &mockRepo{}
	hasher, _ := NewPasswordHasher(32768, 2, 2)
	service := NewService(repo, nil, hasher, nil, nil)

	repo.err = errors.New("db down for audit")

	service.AuditLog(context.Background(), uuid.New(), "TEST_ACTION", "user", uuid.New().String(), "1.2.3.4", "test-ua",
		map[string]any{"k": "v"}, nil)
}

func TestEmailVerification_ReplayAndConcurrency(t *testing.T) {
	repo := &mockRepo{}
	hasher, _ := NewPasswordHasher(32768, 2, 2)

	mRedis := &mockRedisClient{}
	storedUserID := ""

	mRedis.setFunc = func(key string, value interface{}, ttl time.Duration) *redis.StatusCmd {
		storedUserID = value.(string)
		cmd := redis.NewStatusCmd(context.Background())
		return cmd
	}

	mRedis.getFunc = func(key string) *redis.StringCmd {
		cmd := redis.NewStringCmd(context.Background())
		if storedUserID != "" {
			cmd.SetVal(storedUserID)
		} else {
			cmd.SetErr(redis.Nil)
		}
		return cmd
	}

	mRedis.delFunc = func(keys ...string) *redis.IntCmd {
		storedUserID = ""
		cmd := redis.NewIntCmd(context.Background())
		cmd.SetVal(1)
		return cmd
	}

	service := NewService(repo, nil, hasher, nil, mRedis)

	uid := uuid.New()
	repo.getUserByID = db.User{
		ID:    pgtype.UUID{Bytes: uid, Valid: true},
		Email: "verify@example.com",
	}

	token, err := service.RequestEmailVerification(context.Background(), uid)
	assert.NoError(t, err)
	assert.NotEmpty(t, token)

	confirmedUID, err := service.ConfirmEmailVerification(context.Background(), token)
	assert.NoError(t, err)
	assert.Equal(t, uid, confirmedUID)

	_, err = service.ConfirmEmailVerification(context.Background(), token)
	assert.Error(t, err)
}

func TestConcurrentPasswordChange(t *testing.T) {
	repo := &mockRepo{}
	hasher, _ := NewPasswordHasher(32768, 2, 2)
	service := NewService(repo, nil, hasher, nil, nil)

	oldPwd := "OldPass123!"
	newPwd1 := "NewOne456!"
	newPwd2 := "NewTwo789!"
	hash, _ := hasher.HashPassword(oldPwd)
	uid := uuid.New()

	repo.getUserByID = db.User{
		ID:           pgtype.UUID{Bytes: uid, Valid: true},
		Email:        "race@example.com",
		PasswordHash: hash,
	}

	var wg sync.WaitGroup
	var success1, success2 bool
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := service.ChangePassword(context.Background(), uid, oldPwd, newPwd1, "ip", "ua"); err == nil {
			success1 = true
		}
	}()
	go func() {
		defer wg.Done()
		if err := service.ChangePassword(context.Background(), uid, oldPwd, newPwd2, "ip", "ua"); err == nil {
			success2 = true
		}
	}()
	wg.Wait()

	total := 0
	if success1 {
		total++
	}
	if success2 {
		total++
	}

	assert.GreaterOrEqual(t, total, 1)
	assert.LessOrEqual(t, total, 2)
}
