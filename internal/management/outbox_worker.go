package management

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
	"github.com/redis/go-redis/v9"
)

// OutboxWorker implements a high-performance Hybrid CDC-like Transactional Outbox pattern.
// It leverages PostgreSQL LISTEN/NOTIFY for real-time push events to completely eliminate SQL polling database overhead,
// combined with a Decoupled Transaction Pattern that executes external Redis I/O outside of PostgreSQL transactions
// to prevent connection pool starvation and database row lock contention.
type OutboxWorker struct {
	svc *Service
}

func NewOutboxWorker(svc *Service) *OutboxWorker {
	return &OutboxWorker{svc: svc}
}

type CampaignPayload struct {
	CampaignID  string `json:"campaign_id"`
	BudgetLimit int64  `json:"budget_limit,omitempty"`
}

type SettingsPayload struct {
	Settings map[string]string `json:"settings"`
}

func (w *OutboxWorker) Start(ctx context.Context, interval time.Duration) {
	// 1. Cold Sync on startup: Drain any pending events created while the worker was offline
	if err := w.ProcessOutbox(ctx); err != nil {
		slog.Error("outbox startup cold sync failed", "error", err)
	}

	// 2. Persistent LISTEN/NOTIFY background worker (Real-time CDC Push Path)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				}

			// Acquire a dedicated connection for LISTEN
			conn, err := w.svc.pool.Acquire(ctx)
			if err != nil {
				slog.Error("failed to acquire connection for outbox listen, retrying in 2s", "error", err)
				time.Sleep(2 * time.Second)
				continue
			}

			_, err = conn.Exec(ctx, "LISTEN outbox_channel")
			if err != nil {
				conn.Release()
				slog.Error("failed to execute LISTEN on outbox channel, retrying in 2s", "error", err)
				time.Sleep(2 * time.Second)
				continue
			}

			slog.Info("outbox worker listening for real-time events via pg_notify")

			for {
				select {
				case <-ctx.Done():
					conn.Release()
					return
				default:
				}

				// WaitForNotification blocks until a notification is received or context is canceled
				_, err := conn.Conn().WaitForNotification(ctx)
				if err != nil {
					conn.Release()
					if ctx.Err() != nil {
						return
					}
					slog.Error("outbox listen connection lost, reconnecting in 2s", "error", err)
					time.Sleep(2 * time.Second)
					break // Break inner loop to trigger reconnect
				}

				// Real-time edge-triggered signal: drain the queue!
				if err := w.ProcessOutbox(ctx); err != nil {
					slog.Error("failed to process outbox after notification", "error", err)
				}
			}
		}
	}()

	// 3. Fallback Interval Janitor: Resets stuck 'PROCESSING' states and drains missed signals
	ticker := time.NewTicker(interval * 5)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Self-healing: Reset events stuck in 'PROCESSING' state for > 5 minutes back to 'PENDING'
			_, _ = w.svc.pool.Exec(ctx, "UPDATE outbox_events SET status = 'PENDING' WHERE status = 'PROCESSING' AND created_at < NOW() - INTERVAL '5 minutes'")

			// Trigger safety drain
			if err := w.ProcessOutbox(ctx); err != nil {
				if strings.Contains(err.Error(), "closed pool") {
					return
				}
				slog.Error("failed to run safety outbox fallback drain", "error", err)
			}
		}
	}
}

func (w *OutboxWorker) ProcessOutbox(ctx context.Context) error {
	var events []db.OutboxEvent

	// Acquire pending outbox events and transition them to PROCESSING inside a localized transaction.
	// This immediately commits the status update, releasing row locks and returning the DB connection to the pool before executing external I/O.
	err := pgx.BeginFunc(ctx, w.svc.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		var err error
		events, err = q.GetPendingOutboxEventsForUpdate(ctx, 100)
		if err != nil || len(events) == 0 {
			return err
		}

		for _, ev := range events {
			_, err = tx.Exec(ctx, "UPDATE outbox_events SET status = 'PROCESSING' WHERE id = $1", ev.ID)
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil || len(events) == 0 {
		return err
	}

	processedIDs := make([]int64, 0, len(events))
	revertIDs := make([]int64, 0, len(events))

	// Execute Redis network I/O completely outside of the PostgreSQL database transaction.
	// This decouples the database transaction hold time from external network transport latencies to prevent database connection pool exhaustion.
	for _, ev := range events {
		var rdbErr error
		switch ev.EventType {
		case "CREATE_CAMPAIGN":
			var p CampaignPayload
			if err := json.Unmarshal(ev.Payload, &p); err == nil {
				campUUID, _ := uuid.Parse(p.CampaignID)
				rdb := w.svc.getRDB(campUUID)
				if rdb != nil {
					_, rdbErr = rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
						pipe.Set(ctx, fmt.Sprintf("budget:campaign:%s", p.CampaignID), p.BudgetLimit, 24*time.Hour)
						channel := w.svc.cfg.CampaignUpdateChannel
						if channel == "" {
							channel = "campaigns:update"
						}
						pipe.Publish(ctx, channel, p.CampaignID)
						return nil
					})
				}
			}
		case "CANCEL_CAMPAIGN":
			var p CampaignPayload
			if err := json.Unmarshal(ev.Payload, &p); err == nil {
				campUUID, _ := uuid.Parse(p.CampaignID)
				rdb := w.svc.getRDB(campUUID)
				if rdb != nil {
					_, rdbErr = rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
						pipe.Del(ctx, fmt.Sprintf("budget:campaign:%s", p.CampaignID))
						channel := w.svc.cfg.CampaignUpdateChannel
						if channel == "" {
							channel = "campaigns:update"
						}
						pipe.Publish(ctx, channel, p.CampaignID)
						return nil
					})
				}
			}
		case "UPDATE_CAMPAIGN_PACING":
			var p struct {
				CampaignID string `json:"campaign_id"`
				PacingMode string `json:"pacing_mode"`
			}
			if err := json.Unmarshal(ev.Payload, &p); err == nil {
				campUUID, _ := uuid.Parse(p.CampaignID)
				rdb := w.svc.getRDB(campUUID)
				if rdb != nil {
					_, rdbErr = rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
						pipe.HSet(ctx, fmt.Sprintf("campaign:settings:%s", p.CampaignID), "pacing_mode", p.PacingMode)
						channel := w.svc.cfg.CampaignUpdateChannel
						if channel == "" {
							channel = "campaigns:update"
						}
						pipe.Publish(ctx, channel, p.CampaignID)
						return nil
					})
				}
			}
		case "UPDATE_SETTINGS":
			var p SettingsPayload
			if err := json.Unmarshal(ev.Payload, &p); err == nil {
				if len(w.svc.rdbs) > 0 && w.svc.rdbs[0] != nil {
					rdb := w.svc.rdbs[0]
					_, rdbErr = rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
						if len(p.Settings) > 0 {
							pipe.HSet(ctx, "config:values", p.Settings)
						}
						pipe.Incr(ctx, "config:version")
						return nil
					})
				}
			}
		case "CONFIGURE_BRAND_FCAP":
			// Select active campaigns linked to this brand and publish invalidation signals to Redis.
			// This triggers the real-time cache sync in active trackers to immediately apply new brand constraints.
			var p struct {
				BrandID    string `json:"brand_id"`
				FreqLimit  int32  `json:"freq_limit"`
				FreqWindow int32  `json:"freq_window"`
			}
			if err := json.Unmarshal(ev.Payload, &p); err == nil {
				brandUUID, parseErr := uuid.Parse(p.BrandID)
				if parseErr == nil {
					rows, dbErr := w.svc.pool.Query(ctx, "SELECT id FROM campaigns WHERE brand_id = $1 AND status = 'ACTIVE'", ToUUID(brandUUID))
					if dbErr == nil {
						var campIDs []string
						for rows.Next() {
							var cid uuid.UUID
							if scanErr := rows.Scan(&cid); scanErr == nil {
								campIDs = append(campIDs, cid.String())
							}
						}
						rows.Close()

						if len(campIDs) > 0 {
							channel := w.svc.cfg.CampaignUpdateChannel
							if channel == "" {
								channel = "campaigns:update"
							}
							if len(w.svc.rdbs) > 0 && w.svc.rdbs[0] != nil {
								rdb := w.svc.rdbs[0]
								_, rdbErr = rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
									for _, cidStr := range campIDs {
										pipe.Publish(ctx, channel, cidStr)
									}
									return nil
								})
							}
						}
					} else {
						rdbErr = dbErr
					}
				} else {
					rdbErr = parseErr
				}
			} else {
				rdbErr = err
			}
		}

		if rdbErr == nil {
			processedIDs = append(processedIDs, ev.ID)
		} else {
			slog.Warn("redis outbox processing failed for event, marking for revert", "id", ev.ID, "error", rdbErr)
			revertIDs = append(revertIDs, ev.ID)
		}
	}

	// Batch transition the outbox event statuses in the database to finalize the transaction outbox cycle.
	// Executing this as a batch query minimizes database roundtrips and lock holding times.
	if len(processedIDs) > 0 {
		_, err = w.svc.pool.Exec(ctx, "UPDATE outbox_events SET status = 'PROCESSED' WHERE id = ANY($1)", processedIDs)
		if err != nil {
			slog.Error("failed to mark outbox events as processed", "error", err)
		}
	}

	if len(revertIDs) > 0 {
		_, err = w.svc.pool.Exec(ctx, "UPDATE outbox_events SET status = 'PENDING' WHERE id = ANY($1)", revertIDs)
		if err != nil {
			slog.Error("failed to revert failed outbox events", "error", err)
		}
	}

	return nil
}

func ToUUID(u uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: u, Valid: true}
}
