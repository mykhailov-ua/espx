package ads

import (
	"context"
	"strconv"
	"testing"
	"time"

	"espx/internal/domain"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recentImpTSRedis struct {
	mockRedisClient
}

func (m *recentImpTSRedis) Get(ctx context.Context, key string) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx)
	cmd.SetVal(strconv.FormatInt(time.Now().Add(-50*time.Millisecond).UnixMilli(), 10))
	return cmd
}

func TestFraudFilter_LowTTC_ReturnsFraudDetected(t *testing.T) {
	geo := &MockGeoProvider{}
	rdb := &recentImpTSRedis{}
	f := NewFraudFilter(geo, rdb, 300*time.Millisecond)

	evt := &domain.Event{
		Type:       "click",
		UserID:     "user1",
		CampaignID: uuid.New(),
		IP:         "1.1.1.1",
	}

	err := f.Check(context.Background(), evt)
	require.ErrorIs(t, err, ErrFraudDetected)
	assert.Contains(t, evt.FraudReason, "low_ttc:")
}
