package auth

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth/crypto"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth/repository"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth/token"
)

type mockRepo struct {
	repository.Querier
	user repository.User
	err  error
}

func (m *mockRepo) GetUserByEmail(ctx context.Context, email string) (repository.User, error) {
	return m.user, m.err
}

type mockTokenMaker struct {
	token.Maker
	err error
}

func (m *mockTokenMaker) CreateToken(userID uuid.UUID, role string, customerID uuid.UUID, duration time.Duration) (string, error) {
	return "token", m.err
}

func TestServiceMetrics(t *testing.T) {
	repo := &mockRepo{}
	tokenMaker := &mockTokenMaker{}
	hasher := crypto.NewPasswordHasher(65536, 3, 4)
	service := NewService(repo, tokenMaker, hasher)

	repo.err = errors.New("not found")
	_, err := service.Login(context.Background(), "test@example.com", "password", time.Hour)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	metrics := service.GetMetrics()
	if metrics.FailedLoginsTotal != 1 {
		t.Errorf("expected FailedLoginsTotal 1, got %d", metrics.FailedLoginsTotal)
	}
	if metrics.InvalidCredentials != 1 {
		t.Errorf("expected InvalidCredentials 1, got %d", metrics.InvalidCredentials)
	}

	repo.err = nil
	repo.user = repository.User{PasswordHash: "$argon2id$v=19$m=65536,t=3,p=4$some-salt$some-hash"}

	repo.user.PasswordHash = "invalid-hash-format"
	_, err = service.Login(context.Background(), "test@example.com", "password", time.Hour)
	if err == nil {
		t.Fatal("expected error for invalid hash format, got nil")
	}
	metrics = service.GetMetrics()
	if metrics.FailedLoginsTotal != 2 {
		t.Errorf("expected FailedLoginsTotal 2, got %d", metrics.FailedLoginsTotal)
	}
	if metrics.InvalidCredentials != 2 {
		t.Errorf("expected InvalidCredentials 2, got %d", metrics.InvalidCredentials)
	}
}
