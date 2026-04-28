package database

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PartitionManager automates the creation of future partitions and the cleanup
// of old partitions for the 'events' table to maintain high performance and
// bounded disk usage.
type PartitionManager struct {
	pool      *pgxpool.Pool
	retention int
	preCreate int
}

func NewPartitionManager(pool *pgxpool.Pool, retentionDays int, preCreateDays int) *PartitionManager {
	return &PartitionManager{
		pool:      pool,
		retention: retentionDays,
		preCreate: preCreateDays,
	}
}

// Run executes the partition maintenance routine.
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
	err := pm.dropOldPartitions(ctx, dropDate)
	if err != nil {
		return fmt.Errorf("failed to drop old partitions before %s: %w", dropDate.Format("2006-01-02"), err)
	}

	return nil
}

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

func (pm *PartitionManager) dropOldPartitions(ctx context.Context, olderThan time.Time) error {
	query := `
		SELECT child.relname
		FROM pg_inherits
		JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
		JOIN pg_class child ON pg_inherits.inhrelid = child.oid
		JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace
		JOIN pg_namespace nmsp_child ON nmsp_child.oid = child.relnamespace
		WHERE parent.relname = 'events';
	`

	rows, err := pm.pool.Query(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	var partitionsToDrop []string
	prefix := "events_p"
	
	// Format of partition name is events_pYYYY_MM_DD
	thresholdStr := olderThan.Format("2006_01_02")

	for rows.Next() {
		var partitionName string
		if err := rows.Scan(&partitionName); err != nil {
			return err
		}

		if len(partitionName) > len(prefix) && partitionName[:len(prefix)] == prefix {
			dateStr := partitionName[len(prefix):]
			if dateStr < thresholdStr {
				partitionsToDrop = append(partitionsToDrop, partitionName)
			}
		}
	}

	for _, p := range partitionsToDrop {
		slog.Info("dropping old partition", "partition", p)
		safeTableName := pgx.Identifier{p}.Sanitize()
		dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s;", safeTableName)
		if _, err := pm.pool.Exec(ctx, dropQuery); err != nil {
			slog.Error("failed to drop partition", "partition", p, "error", err)
			// Continue trying to drop others even if one fails
		}
	}

	return nil
}

// StartBackground starts a background goroutine that runs maintenance daily.
func (pm *PartitionManager) StartBackground(ctx context.Context) {
	go func() {
		if err := pm.Run(ctx); err != nil {
			slog.Error("initial partition maintenance failed", "error", err)
		} else {
			slog.Info("initial partition maintenance completed")
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
