package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const revocationCheckTimeout = 100 * time.Millisecond

// defaultUserRevocationTTL covers the longest access-token lifetime we issue.
const defaultUserRevocationTTL = 24 * time.Hour

// CheckTokenRevocation consults Redis because access tokens are stateless until explicitly revoked.
// A non-nil error means callers must fail closed rather than accept a token during an outage.
func CheckTokenRevocation(ctx context.Context, rdb redis.UniversalClient, payload *Payload) (revoked bool, err error) {
	if rdb == nil || payload == nil {
		return false, nil
	}

	ctxRevoked, cancel := context.WithTimeout(ctx, revocationCheckTimeout)
	defer cancel()

	cmds, errPipe := rdb.Pipelined(ctxRevoked, func(pipe redis.Pipeliner) error {
		pipe.Exists(ctxRevoked, "revoked:token:"+payload.ID.String())
		pipe.Exists(ctxRevoked, "revoked:session:"+payload.SessionID.String())
		pipe.Exists(ctxRevoked, "revoked:user:"+payload.UserID.String())
		return nil
	})
	if errPipe != nil {
		return false, errPipe
	}
	if len(cmds) != 3 {
		return false, fmt.Errorf("unexpected pipeline commands count: got %d want 3", len(cmds))
	}

	for _, cmd := range cmds {
		intCmd, ok := cmd.(*redis.IntCmd)
		if !ok {
			return false, fmt.Errorf("unexpected pipeline command type")
		}
		exists, errExists := intCmd.Result()
		if errExists != nil {
			return false, errExists
		}
		if exists > 0 {
			return true, nil
		}
	}
	return false, nil
}

// RevokeUserAccess propagates admin blocks to stateless access tokens still within their TTL.
func RevokeUserAccess(ctx context.Context, rdb redis.UniversalClient, userID uuid.UUID, ttl time.Duration) error {
	if rdb == nil {
		return nil
	}
	if ttl <= 0 {
		ttl = defaultUserRevocationTTL
	}
	return rdb.Set(ctx, "revoked:user:"+userID.String(), "1", ttl).Err()
}

// ClearUserRevocation prevents unblocks from leaving stale deny markers in the hot path.
func ClearUserRevocation(ctx context.Context, rdb redis.UniversalClient, userID uuid.UUID) error {
	if rdb == nil {
		return nil
	}
	return rdb.Del(ctx, "revoked:user:"+userID.String()).Err()
}
