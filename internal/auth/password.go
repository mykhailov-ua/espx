package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unsafe"

	"golang.org/x/crypto/argon2"
)

var (
	ErrAuthenticationFailed   = errors.New("authentication failed")
	ErrInvalidPassword        = errors.New("password cannot be empty or exceeds maximum length")
	ErrInsecureHashParameters = errors.New("hash parameters are below minimum security thresholds")
)

const MaxPasswordLength = 72

type params struct {
	memory      uint32
	iterations  uint32
	parallelism uint8
	saltLength  uint32
	keyLength   uint32
}

const (
	maxMemory      uint32 = 256 * 1024
	maxIterations  uint32 = 10
	maxParallelism uint8  = 32
	minSaltLength  uint32 = 16
	minHashLength  uint32 = 32
	minMemory      uint32 = 32768
	minIterations  uint32 = 2
	minParallelism uint8  = 2
)

// PasswordHasher encapsulates Argon2id configuration state and maintains a pre-computed dummy hash.
// The dummy hash neutralizes timing side-channels during authentication of non-existent users.
type PasswordHasher struct {
	memory      uint32
	iterations  uint32
	saltLength  uint32
	keyLength   uint32
	parallelism uint8
	dummyHash   string
}

func NewPasswordHasher(memory, iterations uint32, parallelism uint8) (*PasswordHasher, error) {
	h := &PasswordHasher{
		memory:      memory,
		iterations:  iterations,
		parallelism: parallelism,
		saltLength:  16,
		keyLength:   32,
	}
	var err error
	h.dummyHash, err = h.HashPassword("dummy-password-timing-attack")
	if err != nil {
		return nil, fmt.Errorf("failed to pre-compute dummy hash: %w", err)
	}
	return h, nil
}

func (h *PasswordHasher) GetDummyHash() string {
	return h.dummyHash
}

func (h *PasswordHasher) GetParallelism() uint8 {
	return h.parallelism
}

func (h *PasswordHasher) HashPassword(password string) (string, error) {
	if password == "" || len(password) > MaxPasswordLength {
		return "", ErrInvalidPassword
	}

	salt := make([]byte, h.saltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("failed to generate salt: %w", err)
	}

	hash := argon2.IDKey(
		[]byte(password),
		salt,
		h.iterations,
		h.memory,
		h.parallelism,
		h.keyLength,
	)

	b64Salt := base64.RawStdEncoding.EncodeToString(salt)
	b64Hash := base64.RawStdEncoding.EncodeToString(hash)

	encodedHash := fmt.Sprintf(
		"$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version,
		h.memory,
		h.iterations,
		h.parallelism,
		b64Salt,
		b64Hash,
	)

	return encodedHash, nil
}

func unsafeStringToBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func VerifyPassword(password, encodedHash string) (bool, error) {
	if password == "" || len(password) > MaxPasswordLength {
		return false, ErrAuthenticationFailed
	}

	const prefix = "$argon2id$v="
	if !strings.HasPrefix(encodedHash, prefix) {
		return false, ErrAuthenticationFailed
	}

	idx1 := len(prefix)
	idx2 := strings.IndexByte(encodedHash[idx1:], '$')
	if idx2 == -1 {
		return false, ErrAuthenticationFailed
	}
	idx2 += idx1

	versionStr := encodedHash[idx1:idx2]
	version, err := strconv.Atoi(versionStr)
	if err != nil || version != argon2.Version {
		return false, ErrAuthenticationFailed
	}

	idx3 := idx2 + 1
	idx4 := strings.IndexByte(encodedHash[idx3:], '$')
	if idx4 == -1 {
		return false, ErrAuthenticationFailed
	}
	idx4 += idx3

	paramsStr := encodedHash[idx3:idx4]
	p := params{}

	sIdx := 0
	for sIdx < len(paramsStr) {
		eIdx := strings.IndexByte(paramsStr[sIdx:], ',')
		var part string
		if eIdx == -1 {
			part = paramsStr[sIdx:]
			sIdx = len(paramsStr)
		} else {
			part = paramsStr[sIdx : sIdx+eIdx]
			sIdx += eIdx + 1
		}

		if strings.HasPrefix(part, "m=") {
			m, err := strconv.ParseUint(part[2:], 10, 32)
			if err != nil || m > uint64(maxMemory) {
				return false, ErrAuthenticationFailed
			}
			p.memory = uint32(m)
		} else if strings.HasPrefix(part, "t=") {
			t, err := strconv.ParseUint(part[2:], 10, 32)
			if err != nil || t > uint64(maxIterations) {
				return false, ErrAuthenticationFailed
			}
			p.iterations = uint32(t)
		} else if strings.HasPrefix(part, "p=") {
			pr, err := strconv.ParseUint(part[2:], 10, 8)
			if err != nil || pr > uint64(maxParallelism) {
				return false, ErrAuthenticationFailed
			}
			p.parallelism = uint8(pr)
		} else {
			return false, ErrAuthenticationFailed
		}
	}

	idx5 := idx4 + 1
	idx6 := strings.IndexByte(encodedHash[idx5:], '$')
	if idx6 == -1 {
		return false, ErrAuthenticationFailed
	}
	idx6 += idx5

	b64Salt := encodedHash[idx5:idx6]
	b64Hash := encodedHash[idx6+1:]

	saltLen := base64.RawStdEncoding.DecodedLen(len(b64Salt))
	if uint32(saltLen) < minSaltLength {
		return false, ErrAuthenticationFailed
	}

	hashLen := base64.RawStdEncoding.DecodedLen(len(b64Hash))
	if uint32(hashLen) < minHashLength {
		return false, ErrAuthenticationFailed
	}

	var saltBuf [64]byte
	var hashBuf [128]byte

	if saltLen > len(saltBuf) || hashLen > len(hashBuf) {
		return false, ErrAuthenticationFailed
	}

	nSalt, err := base64.RawStdEncoding.Decode(saltBuf[:], unsafeStringToBytes(b64Salt))
	if err != nil || uint32(nSalt) < minSaltLength {
		return false, ErrAuthenticationFailed
	}
	salt := saltBuf[:nSalt]
	p.saltLength = uint32(nSalt)

	nHash, err := base64.RawStdEncoding.Decode(hashBuf[:], unsafeStringToBytes(b64Hash))
	if err != nil || uint32(nHash) < minHashLength {
		return false, ErrAuthenticationFailed
	}
	hash := hashBuf[:nHash]
	p.keyLength = uint32(nHash)

	var passwordBuf [128]byte
	copy(passwordBuf[:], password)
	passwordBytes := passwordBuf[:len(password)]

	comparisonHash := argon2.IDKey(passwordBytes, salt, p.iterations, p.memory, p.parallelism, p.keyLength)

	if subtle.ConstantTimeCompare(hash, comparisonHash) == 1 {
		if p.memory < minMemory || p.iterations < minIterations || p.parallelism < minParallelism {
			return true, ErrInsecureHashParameters
		}
		return true, nil
	}

	return false, ErrAuthenticationFailed
}
