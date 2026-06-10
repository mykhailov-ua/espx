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

// Events already in the snapshot are skipped via MarkEventIdempotent to prevent double-billing.
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
