package management

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redis/go-redis/v9"
)

// OutboxWorker propagates Postgres transactional changes to Redis for the hot-path ad stack.
type OutboxWorker struct {
	svc *Service
}

// NewOutboxWorker binds outbox processing to the management service.
func NewOutboxWorker(svc *Service) *OutboxWorker {
	return &OutboxWorker{svc: svc}
}

// CampaignPayload carries campaign identity and budget data in outbox events.
type CampaignPayload struct {
	CampaignID  string `json:"campaign_id"`
	BudgetLimit int64  `json:"budget_limit,omitempty"`
}

// SettingsPayload carries system settings snapshots in outbox events.
type SettingsPayload struct {
	Settings map[string]string `json:"settings"`
}

// BlacklistPayload carries IP block or unblock actions in outbox events.
type BlacklistPayload struct {
	Action string `json:"action"`
	IP     string `json:"ip"`
	Reason string `json:"reason"`
}

// normalizeBlacklistReason defaults empty blacklist sources to the manual category.
func normalizeBlacklistReason(reason string) string {
	if reason == "" {
		return "manual"
	}
	return reason
}

// Start runs outbox polling, cold sync, and stale lease recovery until the context is cancelled.
func (w *OutboxWorker) Start(ctx context.Context, interval time.Duration) {
	if err := w.ProcessOutbox(ctx); err != nil {
		slog.Error("outbox startup cold sync failed", "error", err)
	}

	slog.Info("outbox worker starting polling loop", "interval", interval)

	pollTimer := time.NewTimer(interval)
	defer pollTimer.Stop()

	recoveryTicker := time.NewTicker(interval * 5)
	defer recoveryTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-recoveryTicker.C:
			w.reclaimStaleProcessing(ctx)
		case <-pollTimer.C:
			processed, err := w.ProcessOutboxWithCount(ctx, 1000)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if strings.Contains(err.Error(), "closed pool") {
					return
				}
				slog.Error("outbox polling loop iteration failed, retrying in 2s", "error", err)
				pollTimer.Reset(2 * time.Second)
				continue
			}

			if processed > 0 {
				pollTimer.Reset(0)
				continue
			}

			pollTimer.Reset(interval)
		}
	}
}

// reclaimStaleProcessing resets outbox rows stuck in PROCESSING after worker crashes.
func (w *OutboxWorker) reclaimStaleProcessing(ctx context.Context) {
	_, err := w.svc.pool.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'PENDING', processing_started_at = NULL
		WHERE status = 'PROCESSING'
		  AND processing_started_at IS NOT NULL
		  AND processing_started_at < NOW() - INTERVAL '1 minute'`)
	if err != nil && ctx.Err() == nil && !strings.Contains(err.Error(), "closed pool") {
		slog.Error("failed to reclaim stale outbox events", "error", err)
	}
}

// ProcessOutbox drains pending outbox events up to the default batch size.
func (w *OutboxWorker) ProcessOutbox(ctx context.Context) error {
	_, err := w.ProcessOutboxWithCount(ctx, 1000)
	return err
}

// ProcessOutboxWithCount claims, applies, and marks a batch of outbox events, returning the success count.
func (w *OutboxWorker) ProcessOutboxWithCount(ctx context.Context, limit int32) (int, error) {
	opCtx, cancel := workerContext(ctx, workerOutboxTimeout)
	defer cancel()

	var events []db.OutboxEvent

	err := pgx.BeginFunc(opCtx, w.svc.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		var err error
		events, err = q.GetPendingOutboxEventsForUpdate(opCtx, limit)
		if err != nil || len(events) == 0 {
			return err
		}

		ids := make([]int64, len(events))
		for i, ev := range events {
			ids[i] = ev.ID
		}

		_, err = tx.Exec(opCtx, `
			UPDATE outbox_events
			SET status = 'PROCESSING', processing_started_at = NOW()
			WHERE id = ANY($1)`, ids)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil || len(events) == 0 {
		return 0, err
	}

	processedIDs := make([]int64, 0, len(events))
	revertIDs := make([]int64, 0, len(events))

	for _, ev := range events {
		if err := w.handleOutboxEvent(opCtx, ctx, ev); err != nil {
			slog.Warn("redis outbox processing failed for event, marking for revert", "id", ev.ID, "error", err)
			revertIDs = append(revertIDs, ev.ID)
			continue
		}
		processedIDs = append(processedIDs, ev.ID)
	}

	if len(processedIDs) > 0 {
		_, err = w.svc.pool.Exec(opCtx, "UPDATE outbox_events SET status = 'PROCESSED' WHERE id = ANY($1)", processedIDs)
		if err != nil {
			slog.Error("failed to mark outbox events as processed", "error", err)
		}
	}

	if len(revertIDs) > 0 {
		_, err = w.svc.pool.Exec(opCtx, `
			UPDATE outbox_events
			SET status = 'PENDING', processing_started_at = NULL
			WHERE id = ANY($1)`, revertIDs)
		if err != nil {
			slog.Error("failed to revert failed outbox events", "error", err)
		}
	}

	return len(processedIDs), nil
}

// campaignRemainingBudget reads authoritative remaining budget from Postgres for Redis seeding.
func (w *OutboxWorker) campaignRemainingBudget(ctx context.Context, campaignID uuid.UUID) (int64, error) {
	var limit, spend int64
	err := w.svc.pool.QueryRow(ctx, `
		SELECT budget_limit, current_spend
		FROM campaigns
		WHERE id = $1`, ads.ToUUID(campaignID)).Scan(&limit, &spend)
	if err != nil {
		return 0, err
	}
	remaining := limit - spend
	if remaining < 0 {
		remaining = 0
	}
	return remaining, nil
}

// setCampaignBudgetRemaining writes the remaining budget key used by the hot-path auction filter.
func (w *OutboxWorker) setCampaignBudgetRemaining(ctx context.Context, pipe redis.Pipeliner, campaignIDStr string, campaignID uuid.UUID, payloadLimit int64) error {
	remaining, err := w.campaignRemainingBudget(ctx, campaignID)
	if err != nil {
		if payloadLimit <= 0 {
			return err
		}
		remaining = payloadLimit
	}
	if remaining <= 0 {
		return nil
	}
	pipe.Set(ctx, fmt.Sprintf("budget:campaign:%s", campaignIDStr), remaining, 0)
	return nil
}

// ToUUID converts a google/uuid value into the pgtype representation used by raw SQL helpers.
func ToUUID(u uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: u, Valid: true}
}

// applyBlacklistPayload mirrors a blacklist change to every Redis shard.
func (w *OutboxWorker) applyBlacklistPayload(ctx context.Context, p BlacklistPayload) error {
	if len(w.svc.rdbs) == 0 {
		return fmt.Errorf("no redis client available")
	}
	reason := normalizeBlacklistReason(p.Reason)
	key := "blacklist:" + reason
	for _, rdb := range w.svc.rdbs {
		var err error
		switch p.Action {
		case "add":
			err = rdb.SAdd(ctx, key, p.IP).Err()
		case "remove":
			err = rdb.SRem(ctx, key, p.IP).Err()
		default:
			err = fmt.Errorf("unknown blacklist action: %s", p.Action)
		}
		if err != nil {
			return fmt.Errorf("blacklist sync failed on shard: %w", err)
		}
	}
	return nil
}

// syncBrandCreativesToRedis publishes weighted creative lists for hot-path rotation.
func (w *OutboxWorker) syncBrandCreativesToRedis(ctx context.Context, brandIDStr string) error {
	brandID, err := uuid.Parse(brandIDStr)
	if err != nil {
		return err
	}
	rows, err := db.New(w.svc.pool).ListActiveBrandCreatives(ctx, ToUUID(brandID))
	if err != nil {
		return err
	}
	type creativeEntry struct {
		ID     string `json:"id"`
		URL    string `json:"url"`
		Weight int32  `json:"weight"`
	}
	entries := make([]creativeEntry, len(rows))
	for i, r := range rows {
		entries[i] = creativeEntry{
			ID:     uuid.UUID(r.ID.Bytes).String(),
			URL:    r.LandingUrl,
			Weight: r.Weight,
		}
	}
	payload, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	if len(w.svc.rdbs) == 0 {
		return fmt.Errorf("no redis client")
	}
	key := "brand:creatives:" + brandIDStr
	for _, rdb := range w.svc.rdbs {
		if err := rdb.Set(ctx, key, payload, 0).Err(); err != nil {
			return err
		}
	}
	return nil
}
