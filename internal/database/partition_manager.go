package database

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"sync"
)

type dbExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type PartitionManager struct {
	pool      dbExecutor
	retention int
	preCreate int
	wg        sync.WaitGroup
}

func NewPartitionManager(pool dbExecutor, retentionDays int, preCreateDays int) *PartitionManager {
	return &PartitionManager{
		pool:      pool,
		retention: retentionDays,
		preCreate: preCreateDays,
	}
}

func (pm *PartitionManager) Run(ctx context.Context) error {
	// Truncate events_default first; leftover rows cause partition constraint violations.
	if err := pm.truncateDefault(ctx); err != nil {
		slog.Warn("failed to truncate events_default before partition creation", "error", err)
	}

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

	return nil
}

func (pm *PartitionManager) createPartition(ctx context.Context, date time.Time) error {
	tableName := fmt.Sprintf("events_p%s", date.Format("2006_01_02"))
	startDate := date.Format("2006-01-02")
	endDate := date.AddDate(0, 0, 1).Format("2006-01-02")

	safeTableName := pgx.Identifier{tableName}.Sanitize()

	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s PARTITION OF events
		FOR VALUES FROM ('%s') TO ('%s');
	`, safeTableName, startDate, endDate)

	_, err := pm.pool.Exec(ctx, query)
	return err
}

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

func (pm *PartitionManager) truncateDefault(ctx context.Context) error {
	var count int64
	err := pm.pool.QueryRow(ctx, "SELECT count(*) FROM events_default").Scan(&count)
	if err != nil {
		return err
	}

	if count > 0 {
		slog.Error("CRITICAL: events_default partition contains data! Missing future partitions or clock drift detected", "count", count)
	}

	_, err = pm.pool.Exec(ctx, "TRUNCATE TABLE events_default")
	if err != nil {
		return err
	}
	if count > 0 {
		slog.Info("truncated events_default partition to prevent disk exhaustion", "count", count)
	}
	return nil
}

func (pm *PartitionManager) StartBackground(ctx context.Context) {
	pm.wg.Add(1)
	go func() {
		defer pm.wg.Done()
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

func (pm *PartitionManager) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		pm.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
