package ads

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/mykhailov-ua/ad-event-processor/internal/metrics"
)

type PostgresStore struct {
	queries      db.Querier
	writeTimeout time.Duration
}

func NewPostgresStore(queries db.Querier, writeTimeout time.Duration) *PostgresStore {
	return &PostgresStore{
		queries:      queries,
		writeTimeout: writeTimeout,
	}
}

func (s *PostgresStore) StoreBatch(ctx context.Context, events []*domain.Event) error {
	if len(events) == 0 {
		return nil
	}

	clickIDs := make([]string, len(events))
	campaignIDs := make([]pgtype.UUID, len(events))
	userIDs := make([]string, len(events))
	eventTypes := make([]string, len(events))
	payloads := make([][]byte, len(events))
	ipAddresses := make([]string, len(events))
	userAgents := make([]string, len(events))
	createdAts := make([]pgtype.Timestamptz, len(events))
	createdDates := make([]pgtype.Date, len(events))

	defaultPayload := []byte("{}")

	for i, evt := range events {
		clickIDs[i] = evt.ClickID
		campaignIDs[i] = pgtype.UUID{Bytes: evt.CampaignID, Valid: true}
		userIDs[i] = evt.UserID
		eventTypes[i] = evt.Type
		if len(evt.Payload) == 0 {
			payloads[i] = defaultPayload
		} else {
			payloads[i] = evt.Payload
		}
		ipAddresses[i] = evt.IP
		userAgents[i] = evt.UA
		const secondsPerDay = 86400
		unix := evt.CreatedAt.Unix()
		midnight := (unix / secondsPerDay) * secondsPerDay
		createdAts[i] = pgtype.Timestamptz{Time: evt.CreatedAt, Valid: true}
		createdDates[i] = pgtype.Date{
			Time:  time.Unix(midnight, 0).UTC(),
			Valid: true,
		}
	}

	var err error
	waitTime := InitialWait

	for i := 0; i <= MaxRetries; i++ {
		dbCtx, cancel := context.WithTimeout(ctx, s.writeTimeout)
		start := time.Now()

		err = s.queries.InsertEventsBatch(dbCtx, db.InsertEventsBatchParams{
			ClickIds:     clickIDs,
			CampaignIds:  campaignIDs,
			UserIds:      userIDs,
			EventTypes:   eventTypes,
			Payloads:     payloads,
			IpAddresses:  ipAddresses,
			UserAgents:   userAgents,
			CreatedAt:    createdAts,
			CreatedDates: createdDates,
		})

		duration := time.Since(start).Seconds()
		cancel()

		if err == nil {
			metrics.DbWriteDuration.WithLabelValues("postgres").Observe(duration)
			return nil
		}

		if i < MaxRetries {
			timer := time.NewTimer(waitTime)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				waitTime *= 2
				if waitTime > MaxWait {
					waitTime = MaxWait
				}
			}
		}
	}

	metrics.DbWriteErrors.WithLabelValues("postgres").Inc()
	return err
}

func (s *PostgresStore) Close() error {
	return nil
}
