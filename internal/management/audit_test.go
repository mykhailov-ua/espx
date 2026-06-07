package management

import (
	"context"
	"testing"
	"time"

	"espx/internal/database"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuditLog(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanup := database.SetupTestDB(t)
	defer cleanup()

	svc := NewService(pool, nil, nil, nil)
	defer svc.Close()
	ctx := context.Background()

	adminID := uuid.New()
	campaignID := uuid.New()

	t.Run("CreateLog", func(t *testing.T) {
		svc.AuditLog(ctx, nil, adminID, "TEST_ACTION", "campaign", &campaignID, map[string]string{"foo": "bar"}, map[string]string{"ip": "127.0.0.1"})

		var count int
		err := pool.QueryRow(ctx, "SELECT count(*) FROM admin_audit_log WHERE admin_id = $1", adminID).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("Cleanup", func(t *testing.T) {

		oldAdminID := uuid.New()
		_, err := pool.Exec(ctx, "INSERT INTO admin_audit_log (admin_id, action, target_type, created_at) VALUES ($1, $2, $3, $4)",
			oldAdminID, "OLD_ACTION", "system", time.Now().AddDate(0, 0, -100))
		require.NoError(t, err)

		svc.cleanOldLogs(ctx, 90)

		var count int
		err = pool.QueryRow(ctx, "SELECT count(*) FROM admin_audit_log WHERE admin_id = $1", oldAdminID).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})
}
