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

// dbExecutor is the minimal SQL surface PartitionManager needs so tests can mock partition DDL.
type dbExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// PartitionManager keeps the events table partitioned ahead of ingest so rows never pile into events_default.
type PartitionManager struct {
	pool      dbExecutor
	retention int
	preCreate int
	wg        sync.WaitGroup
}

// NewPartitionManager configures retention and lookahead windows for daily event partitions.
func NewPartitionManager(pool dbExecutor, retentionDays int, preCreateDays int) *PartitionManager {
	return &PartitionManager{
		pool:      pool,
		retention: retentionDays,
		preCreate: preCreateDays,
	}
}

// Run pre-creates upcoming partitions and drops expired ones so event inserts always hit a dated child table.
func (pm *PartitionManager) Run(ctx context.Context) error {
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

// createPartition adds one daily child table for the events range partition set.
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

// dropPartitions removes partitions outside retention and pre-create windows to control disk growth.
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

// truncateDefault clears the catch-all partition so stray rows do not block creation of dated child tables.
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

// StartBackground runs partition maintenance on startup and hourly so ingest never outruns partition coverage.
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

// Wait blocks until the background partition worker exits or the context is cancelled.
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
