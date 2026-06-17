package management

import (
	"context"
	"encoding/json"
	"fmt"

	"espx/internal/ads/db"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// handleOutboxEvent dispatches a claimed outbox row to the Redis side-effect handler for its type.
func (w *OutboxWorker) handleOutboxEvent(opCtx, ctx context.Context, ev db.OutboxEvent) error {
	switch ev.EventType {
	case "CREATE_CAMPAIGN":
		return w.handleCreateCampaign(ctx, ev.Payload)
	case "PAUSE_CAMPAIGN":
		return w.handlePauseCampaign(ctx, ev.Payload)
	case "RESUME_CAMPAIGN":
		return w.handleResumeCampaign(ctx, ev.Payload)
	case "UPDATE_CAMPAIGN_SCHEDULE":
		return w.handleUpdateCampaignSchedule(ctx, ev.Payload)
	case "SYNC_BRAND_CREATIVES":
		return w.handleSyncBrandCreatives(ctx, ev.Payload)
	case "CANCEL_CAMPAIGN":
		return w.handleCancelCampaign(ctx, ev.Payload)
	case "UPDATE_CAMPAIGN_PACING":
		return w.handleUpdateCampaignPacing(ctx, ev.Payload)
	case "UPDATE_SETTINGS":
		return w.handleUpdateSettings(opCtx, ev.ID, ev.Payload)
	case "UPDATE_BLACKLIST":
		return w.handleUpdateBlacklist(ctx, ev.Payload)
	case "CONFIGURE_BRAND_FCAP":
		return w.handleConfigureBrandFcap(ctx, ev.Payload)
	default:
		return fmt.Errorf("unknown outbox event type: %s", ev.EventType)
	}
}

// handleCreateCampaign seeds Redis budget keys and publishes a campaign cache invalidation.
func (w *OutboxWorker) handleCreateCampaign(ctx context.Context, payload []byte) error {
	var p CampaignPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("invalid outbox payload: %w", err)
	}
	campUUID, err := uuid.Parse(p.CampaignID)
	if err != nil {
		return fmt.Errorf("invalid campaign id in payload: %w", err)
	}
	rdb := w.svc.getRDB(campUUID)
	if rdb == nil {
		return fmt.Errorf("no redis client available")
	}
	_, err = rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		if err := w.setCampaignBudgetRemaining(ctx, pipe, p.CampaignID, campUUID, p.BudgetLimit); err != nil {
			return err
		}
		pipe.Publish(ctx, w.svc.campaignUpdateChannel(), p.CampaignID)
		return nil
	})
	return err
}

// handlePauseCampaign removes Redis budget keys when delivery stops.
func (w *OutboxWorker) handlePauseCampaign(ctx context.Context, payload []byte) error {
	var p CampaignPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil
	}
	campUUID, _ := uuid.Parse(p.CampaignID)
	return w.deleteCampaignBudgetAndPublish(ctx, p.CampaignID, campUUID)
}

// handleResumeCampaign restores Redis budget keys when delivery resumes.
func (w *OutboxWorker) handleResumeCampaign(ctx context.Context, payload []byte) error {
	var p CampaignPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil
	}
	campUUID, _ := uuid.Parse(p.CampaignID)
	return w.setCampaignBudgetAndPublish(ctx, p, campUUID)
}

// handleUpdateCampaignSchedule notifies the hot path that schedule metadata changed.
func (w *OutboxWorker) handleUpdateCampaignSchedule(ctx context.Context, payload []byte) error {
	var p struct {
		CampaignID string `json:"campaign_id"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil
	}
	campUUID, _ := uuid.Parse(p.CampaignID)
	rdb := w.svc.getRDB(campUUID)
	if rdb == nil {
		return nil
	}
	return rdb.Publish(ctx, w.svc.campaignUpdateChannel(), p.CampaignID).Err()
}

// handleSyncBrandCreatives refreshes weighted landing URLs in Redis for a brand.
func (w *OutboxWorker) handleSyncBrandCreatives(ctx context.Context, payload []byte) error {
	var p struct {
		BrandID string `json:"brand_id"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil
	}
	return w.syncBrandCreativesToRedis(ctx, p.BrandID)
}

// handleCancelCampaign clears Redis budget state when a campaign enters draining cancellation.
func (w *OutboxWorker) handleCancelCampaign(ctx context.Context, payload []byte) error {
	var p CampaignPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil
	}
	campUUID, _ := uuid.Parse(p.CampaignID)
	return w.deleteCampaignBudgetAndPublish(ctx, p.CampaignID, campUUID)
}

// handleUpdateCampaignPacing writes pacing mode to Redis and invalidates campaign caches.
func (w *OutboxWorker) handleUpdateCampaignPacing(ctx context.Context, payload []byte) error {
	var p struct {
		CampaignID string `json:"campaign_id"`
		PacingMode string `json:"pacing_mode"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil
	}
	campUUID, _ := uuid.Parse(p.CampaignID)
	rdb := w.svc.getRDB(campUUID)
	if rdb == nil {
		return nil
	}
	_, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, fmt.Sprintf("campaign:settings:%s", p.CampaignID), "pacing_mode", p.PacingMode)
		pipe.Publish(ctx, w.svc.campaignUpdateChannel(), p.CampaignID)
		return nil
	})
	return err
}

// handleUpdateSettings pushes system settings and a monotonic version to Redis config keys.
func (w *OutboxWorker) handleUpdateSettings(opCtx context.Context, eventID int64, payload []byte) error {
	var p SettingsPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("invalid outbox payload: %w", err)
	}
	if len(w.svc.rdbs) == 0 || w.svc.rdbs[0] == nil {
		return fmt.Errorf("no redis client available")
	}
	rdb := w.svc.rdbs[0]
	_, err := rdb.Pipelined(opCtx, func(pipe redis.Pipeliner) error {
		if len(p.Settings) > 0 {
			pipe.HSet(opCtx, "config:values", p.Settings)
		}
		pipe.Set(opCtx, "config:version", eventID, 0)
		return nil
	})
	return err
}

// handleUpdateBlacklist applies an IP block or unblock to every Redis shard.
func (w *OutboxWorker) handleUpdateBlacklist(ctx context.Context, payload []byte) error {
	var p BlacklistPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("invalid outbox payload: %w", err)
	}
	return w.applyBlacklistPayload(ctx, p)
}

// handleConfigureBrandFcap invalidates active campaigns when brand frequency caps change.
func (w *OutboxWorker) handleConfigureBrandFcap(ctx context.Context, payload []byte) error {
	var p struct {
		BrandID string `json:"brand_id"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}
	brandUUID, err := uuid.Parse(p.BrandID)
	if err != nil {
		return err
	}
	campIDs, err := w.listActiveCampaignIDsByBrand(ctx, brandUUID)
	if err != nil {
		return err
	}
	if len(campIDs) == 0 {
		return nil
	}
	if len(w.svc.rdbs) == 0 || w.svc.rdbs[0] == nil {
		return nil
	}
	channel := w.svc.campaignUpdateChannel()
	rdb := w.svc.rdbs[0]
	_, err = rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, cidStr := range campIDs {
			pipe.Publish(ctx, channel, cidStr)
		}
		return nil
	})
	return err
}

// listActiveCampaignIDsByBrand finds campaigns that must reload brand fcap settings from Redis pubsub.
func (w *OutboxWorker) listActiveCampaignIDsByBrand(ctx context.Context, brandUUID uuid.UUID) ([]string, error) {
	rows, err := w.svc.pool.Query(ctx, "SELECT id FROM campaigns WHERE brand_id = $1 AND status = 'ACTIVE'", ToUUID(brandUUID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var campIDs []string
	for rows.Next() {
		var cid uuid.UUID
		if scanErr := rows.Scan(&cid); scanErr == nil {
			campIDs = append(campIDs, cid.String())
		}
	}
	return campIDs, nil
}

// setCampaignBudgetAndPublish restores budget keys and notifies the hot path on resume or create.
func (w *OutboxWorker) setCampaignBudgetAndPublish(ctx context.Context, p CampaignPayload, campUUID uuid.UUID) error {
	rdb := w.svc.getRDB(campUUID)
	if rdb == nil {
		return nil
	}
	_, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		if err := w.setCampaignBudgetRemaining(ctx, pipe, p.CampaignID, campUUID, p.BudgetLimit); err != nil {
			return err
		}
		pipe.Publish(ctx, w.svc.campaignUpdateChannel(), p.CampaignID)
		return nil
	})
	return err
}

// deleteCampaignBudgetAndPublish removes budget keys and notifies the hot path on pause or cancel.
func (w *OutboxWorker) deleteCampaignBudgetAndPublish(ctx context.Context, campaignIDStr string, campUUID uuid.UUID) error {
	rdb := w.svc.getRDB(campUUID)
	if rdb == nil {
		return nil
	}
	_, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, fmt.Sprintf("budget:campaign:%s", campaignIDStr))
		pipe.Publish(ctx, w.svc.campaignUpdateChannel(), campaignIDStr)
		return nil
	})
	return err
}
