package management

import (
	"context"
	"encoding/json"
	"espx/internal/ads/db"
	"log/slog"
	"time"

	"espx/internal/ads"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

func (s *Service) AuditLog(ctx context.Context, q db.Querier, adminID uuid.UUID, action string, targetType string, targetID *uuid.UUID, changes any, metadata any) {
	changesJSON, _ := json.Marshal(changes)
	metadataJSON, _ := json.Marshal(metadata)

	var tid pgtype.UUID
	if targetID != nil {
		tid = ads.ToUUID(*targetID)
	}

	if q == nil {
		q = db.New(s.pool)
	}

	_, err := q.CreateAuditLog(ctx, db.CreateAuditLogParams{
		AdminID:    ads.ToUUID(adminID),
		Action:     action,
		TargetType: targetType,
		TargetID:   tid,
		Changes:    changesJSON,
		Metadata:   metadataJSON,
	})

	if err != nil {
		slog.Error("failed to write audit log", "error", err, "admin_id", adminID, "action", action)
	}
}

func (s *Service) RunAuditCleaner(ctx context.Context, retention Days) {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.cleanOldLogs(ctx, retention)
		}
	}
}

type Days int

func (s *Service) cleanOldLogs(ctx context.Context, retention Days) {
	threshold := time.Now().AddDate(0, 0, -int(retention))
	err := db.New(s.pool).CleanupAuditLogs(ctx, pgtype.Timestamptz{Time: threshold, Valid: true})
	if err != nil {
		slog.Error("failed to cleanup audit logs", "error", err)
	} else {
		slog.Info("audit logs cleaned up", "older_than", threshold.Format(time.RFC3339))
	}
}
