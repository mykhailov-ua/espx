package management

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"log/slog"
	"time"

	"espx/internal/ads/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type BlacklistDTO struct {
	ID        int64  `json:"id"`
	IP        string `json:"ip"`
	Reason    string `json:"reason"`
	CreatedAt string `json:"created_at"`
}

func (s *Service) BlockIP(ctx context.Context, ip string, source string) error {
	reason := source
	if reason == "" {
		reason = "manual"
	}

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		_, err := q.CreateBlacklistIP(ctx, db.CreateBlacklistIPParams{
			Ip:     ip,
			Reason: reason,
		})
		if err != nil {
			return err
		}

		var uid uuid.UUID
		if u, ok := GetUser(ctx); ok {
			uid = u.UserID
		}
		s.AuditLog(ctx, q, uid, "BLOCK_IP", "system", nil, map[string]string{"ip": ip, "source": reason}, nil)
		return nil
	})
	if err != nil {
		return err
	}

	if len(s.rdbs) == 0 {
		return fmt.Errorf("no redis client available")
	}
	shardIdx := crc32.ChecksumIEEE([]byte(ip)) % uint32(len(s.rdbs))
	rdb := s.rdbs[shardIdx]
	return rdb.SAdd(ctx, "blacklist:"+reason, ip).Err()
}

func (s *Service) UnblockIP(ctx context.Context, ip string, source string) error {
	reason := source
	if reason == "" {
		reason = "manual"
	}

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		err := q.DeleteBlacklistIP(ctx, ip)
		if err != nil {
			return err
		}

		var uid uuid.UUID
		if u, ok := GetUser(ctx); ok {
			uid = u.UserID
		}
		s.AuditLog(ctx, q, uid, "UNBLOCK_IP", "system", nil, map[string]string{"ip": ip, "source": reason}, nil)
		return nil
	})
	if err != nil {
		return err
	}

	if len(s.rdbs) == 0 {
		return fmt.Errorf("no redis client available")
	}
	shardIdx := crc32.ChecksumIEEE([]byte(ip)) % uint32(len(s.rdbs))
	rdb := s.rdbs[shardIdx]
	return rdb.SRem(ctx, "blacklist:"+reason, ip).Err()
}

func (s *Service) UpdateSettings(ctx context.Context, settings map[string]string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		for k, v := range settings {
			err := q.SetSystemSetting(ctx, db.SetSystemSettingParams{
				Key:   k,
				Value: v,
			})
			if err != nil {
				return err
			}
		}

		var uid uuid.UUID
		if u, ok := GetUser(ctx); ok {
			uid = u.UserID
		}
		s.AuditLog(ctx, q, uid, "UPDATE_SETTINGS", "system", nil, settings, nil)
		payloadBytes, _ := json.Marshal(SettingsPayload{Settings: settings})
		_, err := q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{EventType: "UPDATE_SETTINGS", Payload: payloadBytes})
		return err
	})
}

func (s *Service) ListBlacklist(ctx context.Context, limit, offset int32) ([]BlacklistDTO, int64, error) {
	q := db.New(s.pool)
	total, err := q.CountBlacklist(ctx)
	if err != nil {
		return nil, 0, err
	}
	if total == 0 {
		return []BlacklistDTO{}, 0, nil
	}

	rows, err := q.ListBlacklist(ctx, db.ListBlacklistParams{Limit: limit, Offset: offset})
	if err != nil {
		return nil, 0, err
	}

	res := make([]BlacklistDTO, len(rows))
	for i, r := range rows {
		res[i] = BlacklistDTO{
			ID:        r.ID,
			IP:        r.Ip,
			Reason:    r.Reason,
			CreatedAt: r.CreatedAt.Time.Format(time.RFC3339),
		}
	}
	return res, total, nil
}

func (s *Service) GetSettings(ctx context.Context) (map[string]string, error) {
	q := db.New(s.pool)
	rows, err := q.GetAllSystemSettings(ctx)
	if err != nil {
		return nil, err
	}
	res := make(map[string]string)
	for _, r := range rows {
		res[r.Key] = r.Value
	}
	return res, nil
}

func (s *Service) SyncSystemState(ctx context.Context) error {
	q := db.New(s.pool)

	bl, err := q.GetAllBlacklist(ctx)
	if err != nil {
		return fmt.Errorf("failed to get blacklist from db: %w", err)
	}

	if len(s.rdbs) == 0 {
		return fmt.Errorf("no redis client available")
	}

	for _, item := range bl {
		reason := item.Reason
		if reason == "" {
			reason = "manual"
		}
		shardIdx := crc32.ChecksumIEEE([]byte(item.Ip)) % uint32(len(s.rdbs))
		s.rdbs[shardIdx].SAdd(ctx, "blacklist:"+reason, item.Ip)
	}

	st, err := q.GetAllSystemSettings(ctx)
	if err != nil {
		return fmt.Errorf("failed to get settings from db: %w", err)
	}

	if len(st) > 0 {
		settingsMap := make(map[string]string)
		for _, r := range st {
			settingsMap[r.Key] = r.Value
		}
		s.rdbs[0].HSet(ctx, "config:values", settingsMap)
	}

	slog.Info("system state synchronized with redis successfully", "blacklist_items", len(bl), "settings_items", len(st))
	return nil
}

func (s *Service) RunSystemStateSyncer(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	_ = s.SyncSystemState(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.SyncSystemState(ctx); err != nil {
				slog.Error("failed to sync system state", "error", err)
			}
		}
	}
}

func (s *Service) ToggleEmergencyBreaker(ctx context.Context, active bool, reason string) error {
	val := "false"
	if active {
		val = "true"
	}

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		err := q.SetSystemSetting(ctx, db.SetSystemSettingParams{
			Key:   "emergency_breaker",
			Value: val,
		})
		if err != nil {
			return err
		}

		var uid uuid.UUID
		if u, ok := GetUser(ctx); ok {
			uid = u.UserID
		}

		s.AuditLog(ctx, q, uid, "EMERGENCY_BREAKER_TOGGLED", "system", nil, map[string]any{
			"active": active,
			"reason": reason,
		}, nil)

		settings := map[string]string{
			"emergency_breaker": val,
		}
		payloadBytes, _ := json.Marshal(SettingsPayload{Settings: settings})
		_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
			EventType: "UPDATE_SETTINGS",
			Payload:   payloadBytes,
		})
		return err
	})
	return err
}
