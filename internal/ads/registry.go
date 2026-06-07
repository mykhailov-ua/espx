// Package ads implements the in-process campaign registry that provides O(1)
// look-ups by campaign UUID for the filter hot path. The registry stores an
// immutable snapshot inside an atomic.Value (copy-on-write); writers replace the
// entire map atomically so readers never observe partial updates and never block.
//
// Persistence uses two layers: a PostgreSQL source-of-truth read by Sync on start-up
// and on each poll interval, and a JSON file replica written to disk after every
// successful sync. The file replica allows the processor to boot without a live
// database connection if the registry snapshot is recent enough (configurable).
//
// Hot reload is triggered via a Redis Pub/Sub channel (CampaignUpdateChannel);
// any registry mutation published by the management service triggers an immediate
// out-of-band Sync without waiting for the next polling tick.
package ads

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"espx/internal/ads/db"
	"espx/internal/domain"
	"espx/internal/metrics"
	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

type campaignInfo struct {
	campaign *domain.Campaign
	status   db.CampaignStatusType
}

// CampaignRegistry is the in-process cache of active campaign metadata. It is safe
// for concurrent use: readers hold the atomic snapshot pointer while Sync replaces
// the map in one CAS operation. The embedded WaitGroup tracks the background sync
// goroutine started by StartSync.
type CampaignRegistry struct {
	repo          db.Querier
	data          atomic.Value
	manuallyAdded map[uuid.UUID]bool
	mu            sync.Mutex
	replicaPath   string
	wg            sync.WaitGroup
}

type campaignReplicaDTO struct {
	ID               uuid.UUID             `json:"id"`
	CustomerID       uuid.UUID             `json:"customer_id"`
	BrandID          *uuid.UUID            `json:"brand_id,omitempty"`
	BrandFcapKey     string                `json:"brand_fcap_key,omitempty"`
	Name             string                `json:"name"`
	BudgetLimit      int64                 `json:"budget_limit"`
	CurrentSpend     int64                 `json:"current_spend"`
	Status           domain.CampaignStatus `json:"status"`
	PacingMode       domain.PacingMode     `json:"pacing_mode"`
	DailyBudget      int64                 `json:"daily_budget"`
	DailyBudgetMicro int64                 `json:"daily_budget_micro"`
	Timezone         string                `json:"timezone"`
	FreqLimit        int32                 `json:"freq_limit"`
	FreqWindow       int32                 `json:"freq_window"`
	TargetCountries  []string              `json:"target_countries,omitempty"`
	RegistryStatus   string                `json:"registry_status"`
}

func NewRegistry(repo db.Querier) *CampaignRegistry {
	r := &CampaignRegistry{
		manuallyAdded: make(map[uuid.UUID]bool),
		repo:          repo,
		replicaPath:   "campaigns_replica.json",
	}
	r.data.Store(make(map[uuid.UUID]campaignInfo, 100_000))
	return r
}

func (r *CampaignRegistry) SetReplicaPath(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.replicaPath = path
}

func (r *CampaignRegistry) Exists(id uuid.UUID) bool {
	m, _ := r.data.Load().(map[uuid.UUID]campaignInfo)
	if m == nil {
		return false
	}
	info, ok := m[id]
	return ok && info.status == db.CampaignStatusTypeACTIVE
}

func (r *CampaignRegistry) GetCustomerID(campaignID uuid.UUID) (uuid.UUID, bool) {
	m, _ := r.data.Load().(map[uuid.UUID]campaignInfo)
	if m == nil {
		return uuid.Nil, false
	}
	info, ok := m[campaignID]
	if !ok {
		return uuid.Nil, false
	}
	return info.campaign.CustomerID, true
}

// GetCampaign returns the full Campaign record from the snapshot, or (nil, false)
// if the campaign is not in the registry. The returned pointer is valid for the
// lifetime of the atomic snapshot; callers must not mutate it.
func (r *CampaignRegistry) GetCampaign(id uuid.UUID) (*domain.Campaign, bool) {
	m, _ := r.data.Load().(map[uuid.UUID]campaignInfo)
	if m == nil {
		return nil, false
	}
	info, ok := m[id]
	if !ok {
		return nil, false
	}
	return info.campaign, true
}

func (r *CampaignRegistry) Add(id, customerID uuid.UUID, brandID *uuid.UUID, brandFcapKey string, pacingMode domain.PacingMode, dailyBudget int64, timezone string, freqLimit, freqWindow int32, targetCountries []string) {
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		slog.Error("invalid timezone in registry Add", "timezone", timezone, "error", err)
		loc = time.UTC
	}

	var countries map[string]struct{}
	if targetCountries != nil {
		countries = make(map[string]struct{}, len(targetCountries))
		for _, c := range targetCountries {
			countries[c] = struct{}{}
		}
	}

	idStr := id.String()
	customerIDStr := customerID.String()
	dailyBudgetMicro := dailyBudget

	var fcapPrefix string
	if brandFcapKey != "" {
		fcapPrefix = brandFcapKey + ":u:"
	} else {
		fcapPrefix = "fcap:c:" + idStr + ":u:"
	}

	info := campaignInfo{
		campaign: &domain.Campaign{
			ID:                  id,
			IDStr:               idStr,
			IDStrAny:            idStr,
			CustomerID:          customerID,
			CustomerIDStr:       customerIDStr,
			CustomerIDStrAny:    customerIDStr,
			BrandID:             brandID,
			BrandFcapKey:        brandFcapKey,
			PacingMode:          pacingMode,
			DailyBudget:         dailyBudget,
			DailyBudgetMicro:    dailyBudgetMicro,
			DailyBudgetMicroAny: dailyBudgetMicro,
			Timezone:            timezone,
			Location:            loc,
			FreqLimit:           freqLimit,
			FreqLimitAny:        freqLimit,
			FreqWindow:          freqWindow,
			FreqWindowAny:       freqWindow,
			TargetCountries:     countries,
			BudgetCampaignKey:   "budget:campaign:" + idStr,
			CampaignSyncKey:     "budget:sync:campaign:" + idStr,
			CustomerSyncKey:     "budget:sync:customer:" + customerIDStr,
			FcapKeyPrefix:       fcapPrefix,
			DailySpendKeyPrefix: "budget:daily_spent:campaign:" + idStr + ":",
		},
		status: db.CampaignStatusTypeACTIVE,
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	currentMap, _ := r.data.Load().(map[uuid.UUID]campaignInfo)
	newMap := make(map[uuid.UUID]campaignInfo, len(currentMap)+1)
	for k, v := range currentMap {
		newMap[k] = v
	}

	newMap[id] = info
	r.manuallyAdded[id] = true
	r.data.Store(newMap)

	if err := r.saveReplica(newMap); err != nil {
		slog.Error("failed to save local file replica in Add", "error", err)
	}
}

// Sync fetches all active campaigns from PostgreSQL, rebuilds the in-memory map, and
// atomically replaces the atomic.Value snapshot. It writes a JSON replica to the
// configured file path. Returns the number of campaigns loaded and any read or write
// error; callers that cannot tolerate a stale registry on first boot should assert
// (n > 0, err == nil).
func (r *CampaignRegistry) Sync(ctx context.Context) (int, error) {
	rows, err := r.repo.ListActiveCampaigns(ctx)
	if err != nil {
		r.mu.Lock()
		defer r.mu.Unlock()
		currentMap, _ := r.data.Load().(map[uuid.UUID]campaignInfo)
		if len(currentMap) == 0 {
			slog.Warn("postgres sync failed and memory cache is empty, attempting to load from local file replica")
			if loadedMap, loadErr := r.loadReplica(); loadErr == nil {
				r.data.Store(loadedMap)
				return len(loadedMap), nil
			} else {
				slog.Error("failed to load from local file replica", "error", loadErr)
			}
		}
		return 0, err
	}

	fresh := make(map[uuid.UUID]campaignInfo, len(rows))
	for _, row := range rows {
		id := uuid.UUID(row.ID.Bytes)

		if row.UpdatedAt.Valid {
			lag := time.Since(row.UpdatedAt.Time).Seconds()
			if lag >= 0 {
				metrics.RegistrySyncLag.Observe(lag)
			}
		}

		loc, err := time.LoadLocation(row.Timezone)
		if err != nil {
			slog.Warn("failed to load location, fallback to UTC", "campaign", id, "timezone", row.Timezone)
			loc = time.UTC
		}

		customerID := uuid.UUID(row.CustomerID.Bytes)
		dailyBudgetMicro := row.DailyBudget

		var brandIDPtr *uuid.UUID
		if row.BrandID.Valid {
			brandID := uuid.UUID(row.BrandID.Bytes)
			brandIDPtr = &brandID
		}

		idStr := id.String()
		customerIDStr := customerID.String()

		var fcapPrefix string
		if row.BrandFcapKey != "" {
			fcapPrefix = row.BrandFcapKey + ":u:"
		} else {
			fcapPrefix = "fcap:c:" + idStr + ":u:"
		}

		fresh[id] = campaignInfo{
			campaign: &domain.Campaign{
				ID:                  id,
				IDStr:               idStr,
				IDStrAny:            idStr,
				CustomerID:          customerID,
				CustomerIDStr:       customerIDStr,
				CustomerIDStrAny:    customerIDStr,
				BrandID:             brandIDPtr,
				BrandFcapKey:        row.BrandFcapKey,
				PacingMode:          domain.PacingMode(row.PacingMode),
				DailyBudget:         row.DailyBudget,
				DailyBudgetMicro:    dailyBudgetMicro,
				DailyBudgetMicroAny: dailyBudgetMicro,
				Timezone:            row.Timezone,
				Location:            loc,
				FreqLimit:           row.FreqLimit.Int32,
				FreqLimitAny:        row.FreqLimit.Int32,
				FreqWindow:          row.FreqWindow.Int32,
				FreqWindowAny:       row.FreqWindow.Int32,
				TargetCountries:     SliceToMap(row.TargetCountries),
				BudgetCampaignKey:   "budget:campaign:" + idStr,
				CampaignSyncKey:     "budget:sync:campaign:" + idStr,
				CustomerSyncKey:     "budget:sync:customer:" + customerIDStr,
				FcapKeyPrefix:       fcapPrefix,
				DailySpendKeyPrefix: "budget:daily_spent:campaign:" + idStr + ":",
			},
			status: row.Status,
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for id := range fresh {
		delete(r.manuallyAdded, id)
	}
	currentMap, _ := r.data.Load().(map[uuid.UUID]campaignInfo)
	for id := range r.manuallyAdded {
		if info, ok := currentMap[id]; ok {
			fresh[id] = info
		}
	}

	r.data.Store(fresh)

	if err := r.saveReplica(fresh); err != nil {
		slog.Error("failed to save local file replica in Sync", "error", err)
	}

	return len(fresh), nil
}

func (r *CampaignRegistry) saveReplica(m map[uuid.UUID]campaignInfo) error {
	dtos := make([]campaignReplicaDTO, 0, len(m))
	for _, info := range m {
		var targetCountries []string
		if info.campaign.TargetCountries != nil {
			targetCountries = make([]string, 0, len(info.campaign.TargetCountries))
			for c := range info.campaign.TargetCountries {
				targetCountries = append(targetCountries, c)
			}
		}

		dtos = append(dtos, campaignReplicaDTO{
			ID:               info.campaign.ID,
			CustomerID:       info.campaign.CustomerID,
			BrandID:          info.campaign.BrandID,
			BrandFcapKey:     info.campaign.BrandFcapKey,
			Name:             info.campaign.Name,
			BudgetLimit:      info.campaign.BudgetLimit,
			CurrentSpend:     info.campaign.CurrentSpend,
			Status:           info.campaign.Status,
			PacingMode:       info.campaign.PacingMode,
			DailyBudget:      info.campaign.DailyBudget,
			DailyBudgetMicro: info.campaign.DailyBudgetMicro,
			Timezone:         info.campaign.Timezone,
			FreqLimit:        info.campaign.FreqLimit,
			FreqWindow:       info.campaign.FreqWindow,
			TargetCountries:  targetCountries,
			RegistryStatus:   string(info.status),
		})
	}

	data, err := json.Marshal(dtos)
	if err != nil {
		return err
	}

	tempFile := r.replicaPath + ".tmp"
	f, err := os.OpenFile(tempFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {

		if !strings.HasPrefix(r.replicaPath, "/tmp/") {
			r.replicaPath = "/tmp/campaigns_replica.json"
			tempFile = r.replicaPath + ".tmp"
			f, err = os.OpenFile(tempFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		}
		if err != nil {
			return err
		}
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(tempFile)
	}()

	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(tempFile, r.replicaPath)
}

func (r *CampaignRegistry) loadReplica() (map[uuid.UUID]campaignInfo, error) {
	data, err := os.ReadFile(r.replicaPath)
	if err != nil {
		if !strings.HasPrefix(r.replicaPath, "/tmp/") {
			data, err = os.ReadFile("/tmp/campaigns_replica.json")
		}
		if err != nil {
			return nil, err
		}
	}

	var dtos []campaignReplicaDTO
	if err := json.Unmarshal(data, &dtos); err != nil {
		return nil, err
	}

	m := make(map[uuid.UUID]campaignInfo, len(dtos))
	for _, dto := range dtos {
		loc, err := time.LoadLocation(dto.Timezone)
		if err != nil {
			loc = time.UTC
		}

		var countries map[string]struct{}
		if dto.TargetCountries != nil {
			countries = make(map[string]struct{}, len(dto.TargetCountries))
			for _, c := range dto.TargetCountries {
				countries[c] = struct{}{}
			}
		}

		idStr := dto.ID.String()
		customerIDStr := dto.CustomerID.String()

		var fcapPrefix string
		if dto.BrandFcapKey != "" {
			fcapPrefix = dto.BrandFcapKey + ":u:"
		} else {
			fcapPrefix = "fcap:c:" + idStr + ":u:"
		}

		m[dto.ID] = campaignInfo{
			campaign: &domain.Campaign{
				ID:                  dto.ID,
				IDStr:               idStr,
				IDStrAny:            idStr,
				CustomerID:          dto.CustomerID,
				CustomerIDStr:       customerIDStr,
				CustomerIDStrAny:    customerIDStr,
				BrandID:             dto.BrandID,
				BrandFcapKey:        dto.BrandFcapKey,
				Name:                dto.Name,
				BudgetLimit:         dto.BudgetLimit,
				CurrentSpend:        dto.CurrentSpend,
				Status:              dto.Status,
				PacingMode:          dto.PacingMode,
				DailyBudget:         dto.DailyBudget,
				DailyBudgetMicro:    dto.DailyBudgetMicro,
				DailyBudgetMicroAny: dto.DailyBudgetMicro,
				Timezone:            dto.Timezone,
				Location:            loc,
				FreqLimit:           dto.FreqLimit,
				FreqLimitAny:        dto.FreqLimit,
				FreqWindow:          dto.FreqWindow,
				FreqWindowAny:       dto.FreqWindow,
				TargetCountries:     countries,
				BudgetCampaignKey:   "budget:campaign:" + idStr,
				CampaignSyncKey:     "budget:sync:campaign:" + idStr,
				CustomerSyncKey:     "budget:sync:customer:" + customerIDStr,
				FcapKeyPrefix:       fcapPrefix,
				DailySpendKeyPrefix: "budget:daily_spent:campaign:" + idStr + ":",
			},
			status: db.CampaignStatusType(dto.RegistryStatus),
		}
	}
	return m, nil
}

// StartSync launches the background polling and Pub/Sub watch goroutines for the
// registry. Polling fires every interval; Pub/Sub fires on each campaign mutation
// event published by the management service via Redis PUBLISH. The goroutines
// are tracked by the embedded WaitGroup and exit when ctx is cancelled.
func (r *CampaignRegistry) StartSync(ctx context.Context, interval time.Duration) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				count, err := r.Sync(ctx)
				if err != nil {
					slog.Error("campaign registry sync failed", "error", err)
					continue
				}
				slog.Debug("campaign registry synced", "campaigns", count)
			}
		}
	}()
}

func (r *CampaignRegistry) StartWatch(ctx context.Context, rdb redis.UniversalClient, channel string) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		pubsub := rdb.Subscribe(ctx, channel)
		defer pubsub.Close()

		ch := pubsub.Channel(redis.WithChannelSize(1000))
		syncTrigger := make(chan struct{}, 1)

		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-syncTrigger:
					count, err := r.Sync(ctx)
					if err != nil {
						slog.Error("live campaign registry sync failed", "error", err)
					} else {
						slog.Debug("live campaign registry synced via trigger", "campaigns", count)
					}
					time.Sleep(100 * time.Millisecond)
				}
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					slog.Error("redis pubsub channel closed permanently")
					return
				}
				id, err := uuid.Parse(msg.Payload)
				if err != nil {
					slog.Warn("received invalid campaign id in pubsub", "payload", msg.Payload)
					continue
				}
				select {
				case syncTrigger <- struct{}{}:
				default:
				}
				slog.Debug("registry sync triggered via pubsub", "campaign_id", id)
			}
		}
	}()
}

func (r *CampaignRegistry) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
