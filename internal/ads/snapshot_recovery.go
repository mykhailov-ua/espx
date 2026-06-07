// Package ads implements the disaster-recovery snapshot pipeline that bridges
// ClickHouse aggregated spend data and PostgreSQL budget state. The pipeline
// addresses two failure scenarios:
//
//  1. Redis budget key loss (flush, restart): RestoreSnapshot reads the
//     ClickHouse aggregates up to a checkpoint time, writes current_spend
//     to PostgreSQL, and seeds Redis budget:campaign:* keys with
//     (budget_limit - spend) so the edge Lua filter resumes correctly.
//
//  2. Processor downtime with unbounded Redis accumulation: ReplayTelemetrySince
//     re-runs all raw events since the checkpoint through UnifiedFilter with
//     idempotency guarded by MarkEventIdempotent in PostgreSQL, preventing
//     double-billing of events already reflected in the snapshot.
//
// CreateSnapshot serialises a Snapshot struct (checkpoint time + campaign spend map)
// to JSON; the bytes are intended for durable storage (S3, GCS) by the caller.
package ads

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"espx/internal/domain"
	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

type Snapshot struct {
	CheckpointTime time.Time           `json:"checkpoint_time"`
	CampaignSpends map[uuid.UUID]int64 `json:"campaign_spends"`
}

type ClickHouseConn interface {
	QueryEventsSince(ctx context.Context, since time.Time) ([]*domain.Event, error)
	QueryAggregatedSpend(ctx context.Context, until time.Time) (map[uuid.UUID]int64, error)
}

type PostgresConn interface {
	UpdateCampaignSpend(ctx context.Context, campaignID uuid.UUID, currentSpend int64) error
	GetCampaignBudgetLimit(ctx context.Context, campaignID uuid.UUID) (int64, error)
	GetCampaignSpend(ctx context.Context, campaignID uuid.UUID) (int64, error)
	MarkEventIdempotent(ctx context.Context, clickID string) (bool, error)
}

// SnapshotReplicator orchestrates the snapshot create, restore, and replay
// operations. rdbs holds the sharded Redis slice used to seed budget keys
// after restore; sharder routes each campaign ID to the correct shard.
type SnapshotReplicator struct {
	mu          sync.RWMutex
	pgConn      PostgresConn
	chConn      ClickHouseConn
	rdbs        []redis.UniversalClient
	sharder     Sharder
	clickCharge int64
	impCharge   int64
}

func NewSnapshotReplicator(
	pg PostgresConn,
	ch ClickHouseConn,
	rdbs []redis.UniversalClient,
	sharder Sharder,
	clickCharge, impCharge int64,
) *SnapshotReplicator {
	return &SnapshotReplicator{
		pgConn:      pg,
		chConn:      ch,
		rdbs:        rdbs,
		sharder:     sharder,
		clickCharge: clickCharge,
		impCharge:   impCharge,
	}
}

// CreateSnapshot queries ClickHouse for aggregated spend per campaign up to until,
// serialises the result as JSON, and returns the raw bytes. The mu write lock
// prevents concurrent snapshot operations from racing on the ClickHouse connection.
func (sr *SnapshotReplicator) CreateSnapshot(ctx context.Context, until time.Time) ([]byte, error) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	spends, err := sr.chConn.QueryAggregatedSpend(ctx, until)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch aggregates from ClickHouse: %w", err)
	}

	snap := &Snapshot{
		CheckpointTime: until,
		CampaignSpends: spends,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize snapshot: %w", err)
	}

	return data, nil
}

// RestoreSnapshot deserialises snapshotData, writes current_spend to PostgreSQL for
// each campaign, and seeds the Redis budget key with remaining = budget_limit - spend.
// Returns the parsed Snapshot for use as a checkpoint timestamp in ReplayTelemetrySince.
func (sr *SnapshotReplicator) RestoreSnapshot(ctx context.Context, snapshotData []byte) (*Snapshot, error) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	var snap Snapshot
	if err := json.Unmarshal(snapshotData, &snap); err != nil {
		return nil, fmt.Errorf("failed to parse snapshot data: %w", err)
	}

	for campID, spend := range snap.CampaignSpends {

		if err := sr.pgConn.UpdateCampaignSpend(ctx, campID, spend); err != nil {
			return nil, fmt.Errorf("failed to update postgres campaign spend for %s: %w", campID, err)
		}

		limit, err := sr.pgConn.GetCampaignBudgetLimit(ctx, campID)
		if err != nil {
			return nil, fmt.Errorf("failed to get campaign limit for %s: %w", campID, err)
		}

		remaining := limit - spend
		if remaining < 0 {
			remaining = 0
		}

		budgetKey := fmt.Sprintf("budget:campaign:%s", campID)
		shardIdx := sr.sharder.GetShard(campID)
		rdb := sr.rdbs[shardIdx%len(sr.rdbs)]

		if err := rdb.Set(ctx, budgetKey, remaining, 24*time.Hour).Err(); err != nil {
			return nil, fmt.Errorf("failed to seed redis budget for %s: %w", campID, err)
		}
	}

	return &snap, nil
}

// ReplayTelemetrySince replays all raw events recorded since since through the
// UnifiedFilter. Each event is first checked for idempotency in PostgreSQL;
// duplicate events are skipped. ErrBudgetExhausted events are silently dropped
// (budget already exhausted in the restored state); all other errors abort the replay.
func (sr *SnapshotReplicator) ReplayTelemetrySince(ctx context.Context, since time.Time, f *UnifiedFilter) (int, error) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	events, err := sr.chConn.QueryEventsSince(ctx, since)
	if err != nil {
		return 0, fmt.Errorf("failed to query raw telemetry since %s: %w", since, err)
	}

	replayedCount := 0
	for _, e := range events {

		isNew, err := sr.pgConn.MarkEventIdempotent(ctx, e.ClickID)
		if err != nil {
			return replayedCount, fmt.Errorf("failed to execute idempotency check for %s: %w", e.ClickID, err)
		}
		if !isNew {

			continue
		}

		err = f.Check(ctx, e)
		if err != nil {

			if err == ErrBudgetExhausted {
				continue
			}
			return replayedCount, fmt.Errorf("failed to replay event %s: %w", e.ClickID, err)
		}

		charge := sr.clickCharge
		if e.Type == "impression" {
			charge = sr.impCharge
		}
		currentSpend, err := sr.pgConn.GetCampaignSpend(ctx, e.CampaignID)
		if err == nil {
			_ = sr.pgConn.UpdateCampaignSpend(ctx, e.CampaignID, currentSpend+charge)
		}

		replayedCount++
	}

	return replayedCount, nil
}
