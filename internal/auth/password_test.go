package auth

import (
	"strings"
	"testing"
)

func TestHashAndVerify(t *testing.T) {
	password := "super_secret_password_123!"

	hasher, err := NewPasswordHasher(65536, 3, 4)
	if err != nil {
		t.Fatalf("NewPasswordHasher failed: %v", err)
	}
	hash, err := hasher.HashPassword(password)
	if err != nil {
		t.Fatalf("HashPassword failed: %v", err)
	}

	if !strings.HasPrefix(hash, "$argon2id$v=19$m=65536,t=3,p=4$") {
		t.Errorf("Unexpected hash format: %s", hash)
	}

	match, err := VerifyPassword(password, hash)
	if err != nil {
		t.Fatalf("VerifyPassword failed: %v", err)
	}
	if !match {
		t.Error("VerifyPassword returned false for correct password")
	}

	_, err = hasher.HashPassword("")
	if err != ErrInvalidPassword {
		t.Errorf("Expected ErrInvalidPassword for empty string, got %v", err)
	}
	match, err = VerifyPassword("", hash)
	if match || err != ErrAuthenticationFailed {
		t.Errorf("Expected ErrAuthenticationFailed for empty password verification, got %v", err)
	}
}

func TestVerifyPassword_SecurityBoundsAndFormats(t *testing.T) {
	tests := []struct {
		name string
		hash string
	}{
		{"Empty hash", ""},
		{"Missing parts", "$argon2id$v=19$m=65536,t=3,p=4$"},
		{"Wrong algorithm", "$argon2i$v=19$m=65536,t=3,p=4$salt$hash"},
		{"Incompatible version", "$argon2id$v=18$m=65536,t=3,p=4$salt$hash"},
		{"Memory exceeds limit", "$argon2id$v=19$m=262145,t=3,p=4$c2FsdHNhbHRzYWx0c2FsdA$aGFzaGhhc2hoYXNoaGFzaGhhc2hoYXNoaGFzaGhhc2g"},
		{"Iterations exceed limit", "$argon2id$v=19$m=65536,t=11,p=4$c2FsdHNhbHRzYWx0c2FsdA$aGFzaGhhc2hoYXNoaGFzaGhhc2hoYXNoaGFzaGhhc2g"},
		{"Parallelism exceeds limit", "$argon2id$v=19$m=65536,t=3,p=33$c2FsdHNhbHRzYWx0c2FsdA$aGFzaGhhc2hoYXNoaGFzaGhhc2hoYXNoaGFzaGhhc2g"},
		{"Salt too short", "$argon2id$v=19$m=65536,t=3,p=4$c2hvcnQ$aGFzaGhhc2hoYXNoaGFzaGhhc2hoYXNoaGFzaGhhc2g"},
		{"Hash too short", "$argon2id$v=19$m=65536,t=3,p=4$c2FsdHNhbHRzYWx0c2FsdA$c2hvcnQ"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			match, err := VerifyPassword("any_password", tt.hash)
			if match {
				t.Error("Expected match to be false on invalid/malicious format")
			}
			if err != ErrAuthenticationFailed {
				t.Errorf("Expected ErrAuthenticationFailed, got %v", err)
			}
		})
	}
}

func BenchmarkHashPassword(b *testing.B) {
	password := "benchmark_password"
	b.ResetTimer()
	b.ReportAllocs()

	hasher, err := NewPasswordHasher(65536, 3, 4)
	if err != nil {
		b.Fatalf("NewPasswordHasher failed: %v", err)
	}
	for i := 0; i < b.N; i++ {
		_, _ = hasher.HashPassword(password)
	}
}

func BenchmarkVerifyPassword(b *testing.B) {
	password := "benchmark_password"
	hasher, err := NewPasswordHasher(65536, 3, 4)
	if err != nil {
		b.Fatalf("NewPasswordHasher failed: %v", err)
	}
	hash, _ := hasher.HashPassword(password)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = VerifyPassword(password, hash)
	}
}
