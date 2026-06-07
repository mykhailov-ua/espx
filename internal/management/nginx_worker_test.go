package management

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"espx/internal/database"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNginxConfigWorker(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	svc := NewService(pool, []redis.UniversalClient{rdb}, nil, nil)
	defer svc.Close()

	exportPath := t.TempDir()
	worker := NewNginxConfigWorker(svc, exportPath)

	ctx := context.Background()

	t.Run("ExportBlacklists", func(t *testing.T) {
		err := svc.BlockIP(ctx, "1.2.3.4", "manual")
		require.NoError(t, err)
		err = svc.BlockIP(ctx, "5.6.7.8", "auto")
		require.NoError(t, err)

		err = worker.ExportAndReload(ctx)
		require.NoError(t, err)

		manualContent, err := os.ReadFile(filepath.Join(exportPath, "manual.conf"))
		require.NoError(t, err)
		assert.Contains(t, string(manualContent), "deny 1.2.3.4;")

		autoContent, err := os.ReadFile(filepath.Join(exportPath, "auto.conf"))
		require.NoError(t, err)
		assert.Contains(t, string(autoContent), "deny 5.6.7.8;")

		flagPath := filepath.Join(exportPath, "reload_required.flg")
		flagInfo, err := os.Stat(flagPath)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0644), flagInfo.Mode().Perm())

		flagContent, err := os.ReadFile(flagPath)
		require.NoError(t, err)
		assert.Equal(t, "1\n", string(flagContent))
	})

	t.Run("IPValidationAndInjectionPrevention", func(t *testing.T) {
		worker := &NginxConfigWorker{exportPath: t.TempDir()}

		ips := []string{
			"1.2.3.4",
			"5.6.7.8/24",
			"10.0.0.999",
			"1.2.3.4;\ninclude /etc/nginx/nginx.conf;\n#",
			"2001:db8::1",
			"2001:db8::/32",
		}

		err := worker.writeDenyFile("test_validation.conf", ips)
		require.NoError(t, err)

		contentBytes, err := os.ReadFile(filepath.Join(worker.exportPath, "test_validation.conf"))
		require.NoError(t, err)
		content := string(contentBytes)

		assert.Contains(t, content, "deny 1.2.3.4;\n")
		assert.Contains(t, content, "deny 5.6.7.8/24;\n")
		assert.Contains(t, content, "deny 2001:db8::1;\n")
		assert.Contains(t, content, "deny 2001:db8::/32;\n")

		assert.NotContains(t, content, "10.0.0.999")
		assert.NotContains(t, content, "include")
	})
}

func BenchmarkNginxConfigWorker_writeDenyFile(b *testing.B) {
	worker := &NginxConfigWorker{exportPath: b.TempDir()}
	ips := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		ips[i] = "192.168.1.1"
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = worker.writeDenyFile("test.conf", ips)
	}
}
