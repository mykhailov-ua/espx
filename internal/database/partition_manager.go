package database

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type dbExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// PartitionManager automates the creation of future partitions and the cleanup
// of old partitions for the 'events' table to maintain high performance and
// bounded disk usage.
type PartitionManager struct {
	pool      dbExecutor
	retention int
	preCreate int
}

func NewPartitionManager(pool dbExecutor, retentionDays int, preCreateDays int) *PartitionManager {
	return &PartitionManager{
		pool:      pool,
		retention: retentionDays,
		preCreate: preCreateDays,
	}
}

// Run executes the partition maintenance routine.
// It creates future partitions to ensure data can always be inserted and
// drops partitions older than the retention period to reclaim disk space.
func (pm *PartitionManager) Run(ctx context.Context) error {
	now := time.Now().UTC()

	for i := 0; i <= pm.preCreate; i++ {
		targetDate := now.AddDate(0, 0, i)
		err := pm.createPartition(ctx, targetDate)
		if err != nil {
			return fmt.Errorf("failed to create partition for %s: %w", targetDate.Format("2006-01-02"), err)
		}
	}

	dropDate := now.AddDate(0, 0, -pm.retention)
	err := pm.dropPartitions(ctx, now, dropDate)
	if err != nil {
		return fmt.Errorf("failed to drop partitions: %w", err)
	}

	if err := pm.truncateDefault(ctx); err != nil {
		slog.Warn("failed to truncate events_default", "error", err)
	}

	return nil
}

// createPartition generates a new hourly/daily partition for the events table.
// It uses IF NOT EXISTS to be idempotent and safe for concurrent execution.
func (pm *PartitionManager) createPartition(ctx context.Context, date time.Time) error {
	tableName := fmt.Sprintf("events_p%s", date.Format("2006_01_02"))
	startDate := date.Format("2006-01-02")
	endDate := date.AddDate(0, 0, 1).Format("2006-01-02")

	// Sanitize table name to prevent SQL injection
	safeTableName := pgx.Identifier{tableName}.Sanitize()

	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s PARTITION OF events
		FOR VALUES FROM ('%s') TO ('%s');
	`, safeTableName, startDate, endDate)

	_, err := pm.pool.Exec(ctx, query)
	return err
}

// dropPartitions identifies and deletes tables that fall outside the retention window
// or are too far in the future.
func (pm *PartitionManager) dropPartitions(ctx context.Context, now time.Time, olderThan time.Time) error {
	query := `
		SELECT child.relname
		FROM pg_inherits
		JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
		JOIN pg_class child ON pg_inherits.inhrelid = child.oid
		WHERE parent.relname = 'events';
	`

	rows, err := pm.pool.Query(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	var partitionsToDrop []string
	prefix := "events_p"

	thresholdStr := olderThan.Format("2006_01_02")
	futureThresholdStr := now.AddDate(0, 0, pm.preCreate).Format("2006_01_02")

	for rows.Next() {
		var partitionName string
		if err := rows.Scan(&partitionName); err != nil {
			return err
		}

		if len(partitionName) == 18 && partitionName[:len(prefix)] == prefix {
			dateStr := partitionName[len(prefix):]
			// Drop if older than retention or too far in the future
			if dateStr < thresholdStr || dateStr > futureThresholdStr {
				partitionsToDrop = append(partitionsToDrop, partitionName)
			}
		}
	}

	for _, p := range partitionsToDrop {
		slog.Info("dropping partition", "partition", p)
		safeTableName := pgx.Identifier{p}.Sanitize()
		dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s;", safeTableName)
		if _, err := pm.pool.Exec(ctx, dropQuery); err != nil {
			slog.Error("failed to drop partition", "partition", p, "error", err)
		}
	}

	return nil
}

// truncateDefault clears the default partition that catches events not matching
// any date-based partition. Without periodic cleanup, this table grows unbounded
// and degrades query performance via sequential scans.
func (pm *PartitionManager) truncateDefault(ctx context.Context) error {
	_, err := pm.pool.Exec(ctx, "TRUNCATE TABLE events_default")
	if err != nil {
		return err
	}
	slog.Info("truncated events_default partition")
	return nil
}

// StartBackground starts a background goroutine that runs maintenance daily.
func (pm *PartitionManager) StartBackground(ctx context.Context) {
	go func() {
		if err := pm.Run(ctx); err != nil {
			slog.Error("initial partition maintenance failed", "error", err)
		}

		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := pm.Run(ctx); err != nil {
					slog.Error("background partition maintenance failed", "error", err)
				}
			}
		}
	}()
}
