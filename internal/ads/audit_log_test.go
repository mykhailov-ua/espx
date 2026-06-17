package ads

import (
	"sync/atomic"
	"testing"
	"time"

	"espx/pkg/logger"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Guards audit log sample mask derives correctly from config ratio.
func TestAuditLogSampleMaskFromConfig(t *testing.T) {
	assert.Equal(t, uint64(0), auditLogSampleMaskFromConfig(-1))
	assert.Equal(t, uint64(127), auditLogSampleMaskFromConfig(0))
	assert.Equal(t, uint64(63), auditLogSampleMaskFromConfig(63))
}

// Guards audit log priority ordering favors critical events over impressions.
func TestAuditLogPriority(t *testing.T) {
	assert.Equal(t, uint8(1), auditLogPriority("click"))
	assert.Equal(t, uint8(1), auditLogPriority("conversion"))
	assert.Equal(t, uint8(0), auditLogPriority("impression"))
}

// Guards impression audit logs respect sampling mask to control volume.
func TestWriteAuditLog_impressionSampling(t *testing.T) {
	cfg := logger.Config{
		LogDir:           t.TempDir(),
		FlushBufferSize:  4096,
		RotateSize:       1024 * 1024,
		RotateInterval:   time.Hour,
		DiskLatencyLimit: time.Second,
	}
	l := logger.NewLogger(cfg, 1)
	defer l.Close()

	var seq atomic.Uint64
	campID := uuid.New()
	const n = 128 * 50
	for i := 0; i < n; i++ {
		writeAuditLog(l, &seq, 127, 0, time.Now().Unix(), campID, "click-id", "impression")
	}
	want := n / 128
	got := l.Shards()[0].WriteCursor()
	assert.InDelta(t, want, got, 3)
}

// Guards critical audit events are never dropped by sampling.
func TestWriteAuditLog_criticalNotSampled(t *testing.T) {
	cfg := logger.Config{
		LogDir:           t.TempDir(),
		FlushBufferSize:  4096,
		RotateSize:       1024 * 1024,
		RotateInterval:   time.Hour,
		DiskLatencyLimit: time.Second,
	}
	l := logger.NewLogger(cfg, 1)
	defer l.Close()

	var seq atomic.Uint64
	campID := uuid.New()
	const n = 64
	for i := 0; i < n; i++ {
		writeAuditLog(l, &seq, 127, 0, time.Now().Unix(), campID, "click-id", "click")
	}
	got := l.Shards()[0].WriteCursor()
	require.Equal(t, uint64(n), got)
}
