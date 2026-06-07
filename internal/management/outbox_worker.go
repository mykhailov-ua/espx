// Package management implements OutboxWorker, which reads PENDING rows from
// outbox_events and applies their side-effects to Redis (budget key creation,
// campaign settings publication, brand frequency cap updates). The worker uses
// PostgreSQL LISTEN on outbox_channel for real-time notifications, with a
// 5x interval ticker as a safety fallback for missed notifications.
//
// Processing protocol:
//  1. BEGIN, SELECT ... FOR UPDATE SKIP LOCKED (up to 100 rows).
//  2. Set status = 'PROCESSING'; COMMIT.
//  3. Apply side-effects to Redis via Pipelined.
//  4. On success: UPDATE status = 'PROCESSED'.
//     On Redis failure: UPDATE status = 'PENDING' (revert for retry).
//
// Stale PROCESSING rows (older than 5 minutes) are reset to PENDING by the
// ticker to handle crashes that occurred between steps 2 and 4.
package management

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"espx/internal/ads/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redis/go-redis/v9"
)

// OutboxWorker drains the outbox_events table and propagates campaign and settings
// mutations to Redis. It is safe to run as a single instance; the SELECT FOR UPDATE
// SKIP LOCKED ensures concurrent instances do not process the same row.
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

	if err := w.ProcessOutbox(ctx); err != nil {
		slog.Error("outbox startup cold sync failed", "error", err)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

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

				_, err := conn.Conn().WaitForNotification(ctx)
				if err != nil {
					conn.Release()
					if ctx.Err() != nil {
						return
					}
					slog.Error("outbox listen connection lost, reconnecting in 2s", "error", err)
					time.Sleep(2 * time.Second)
					break
				}

				if err := w.ProcessOutbox(ctx); err != nil {
					slog.Error("failed to process outbox after notification", "error", err)
				}
			}
		}
	}()

	ticker := time.NewTicker(interval * 5)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:

			_, _ = w.svc.pool.Exec(ctx, "UPDATE outbox_events SET status = 'PENDING' WHERE status = 'PROCESSING' AND created_at < NOW() - INTERVAL '5 minutes'")

			if err := w.ProcessOutbox(ctx); err != nil {
				if strings.Contains(err.Error(), "closed pool") {
					return
				}
				slog.Error("failed to run safety outbox fallback drain", "error", err)
			}
		}
	}
}

// ProcessOutbox reads up to 100 PENDING outbox events in a single transaction,
// marks them PROCESSING, then applies Redis side-effects. Rows that fail Redis
// application are reverted to PENDING; successful rows are marked PROCESSED.
// Returns the first database-level error; Redis errors are logged and retried on
// the next ProcessOutbox call.
func (w *OutboxWorker) ProcessOutbox(ctx context.Context) error {
	var events []db.OutboxEvent

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
