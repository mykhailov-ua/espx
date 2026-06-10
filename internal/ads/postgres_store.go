package ads

import (
	"context"
	"sync"
	"time"

	"espx/internal/ads/db"
	"espx/internal/domain"
	"espx/internal/metrics"
	"github.com/jackc/pgx/v5/pgtype"
)

type postgresBatchArrays struct {
	clickIDs     []string
	campaignIDs  []pgtype.UUID
	userIDs      []string
	eventTypes   []string
	payloads     [][]byte
	ipAddresses  []string
	userAgents   []string
	createdAts   []pgtype.Timestamptz
	createdDates []pgtype.Date
}

var postgresBatchArraysPool = sync.Pool{
	New: func() any {
		return &postgresBatchArrays{
			clickIDs:     make([]string, 0, 1000),
			campaignIDs:  make([]pgtype.UUID, 0, 1000),
			userIDs:      make([]string, 0, 1000),
			eventTypes:   make([]string, 0, 1000),
			payloads:     make([][]byte, 0, 1000),
			ipAddresses:  make([]string, 0, 1000),
			userAgents:   make([]string, 0, 1000),
			createdAts:   make([]pgtype.Timestamptz, 0, 1000),
			createdDates: make([]pgtype.Date, 0, 1000),
		}
	},
}

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

	arrs := postgresBatchArraysPool.Get().(*postgresBatchArrays)
	defer func() {
		for i := range arrs.clickIDs {
			arrs.clickIDs[i] = ""
		}
		arrs.clickIDs = arrs.clickIDs[:0]

		for i := range arrs.campaignIDs {
			arrs.campaignIDs[i] = pgtype.UUID{}
		}
		arrs.campaignIDs = arrs.campaignIDs[:0]

		for i := range arrs.userIDs {
			arrs.userIDs[i] = ""
		}
		arrs.userIDs = arrs.userIDs[:0]

		for i := range arrs.eventTypes {
			arrs.eventTypes[i] = ""
		}
		arrs.eventTypes = arrs.eventTypes[:0]

		for i := range arrs.payloads {
			arrs.payloads[i] = nil
		}
		arrs.payloads = arrs.payloads[:0]

		for i := range arrs.ipAddresses {
			arrs.ipAddresses[i] = ""
		}
		arrs.ipAddresses = arrs.ipAddresses[:0]

		for i := range arrs.userAgents {
			arrs.userAgents[i] = ""
		}
		arrs.userAgents = arrs.userAgents[:0]

		for i := range arrs.createdAts {
			arrs.createdAts[i] = pgtype.Timestamptz{}
		}
		arrs.createdAts = arrs.createdAts[:0]

		for i := range arrs.createdDates {
			arrs.createdDates[i] = pgtype.Date{}
		}
		arrs.createdDates = arrs.createdDates[:0]

		postgresBatchArraysPool.Put(arrs)
	}()

	n := len(events)
	if cap(arrs.clickIDs) < n {
		arrs.clickIDs = make([]string, 0, n)
		arrs.campaignIDs = make([]pgtype.UUID, 0, n)
		arrs.userIDs = make([]string, 0, n)
		arrs.eventTypes = make([]string, 0, n)
		arrs.payloads = make([][]byte, 0, n)
		arrs.ipAddresses = make([]string, 0, n)
		arrs.userAgents = make([]string, 0, n)
		arrs.createdAts = make([]pgtype.Timestamptz, 0, n)
		arrs.createdDates = make([]pgtype.Date, 0, n)
	}

	defaultPayload := []byte("{}")

	for _, evt := range events {
		arrs.clickIDs = append(arrs.clickIDs, evt.ClickID)
		arrs.campaignIDs = append(arrs.campaignIDs, pgtype.UUID{Bytes: evt.CampaignID, Valid: true})
		arrs.userIDs = append(arrs.userIDs, evt.UserID)
		arrs.eventTypes = append(arrs.eventTypes, evt.Type)
		if len(evt.Payload) == 0 {
			arrs.payloads = append(arrs.payloads, defaultPayload)
		} else {
			arrs.payloads = append(arrs.payloads, evt.Payload)
		}
		arrs.ipAddresses = append(arrs.ipAddresses, evt.IP)
		arrs.userAgents = append(arrs.userAgents, evt.UA)

		const secondsPerDay = 86400
		unix := evt.CreatedAt.Unix()
		midnight := (unix / secondsPerDay) * secondsPerDay
		arrs.createdAts = append(arrs.createdAts, pgtype.Timestamptz{Time: evt.CreatedAt, Valid: true})
		arrs.createdDates = append(arrs.createdDates, pgtype.Date{
			Time:  time.Unix(midnight, 0).UTC(),
			Valid: true,
		})
	}

	var err error
	waitTime := InitialWait

	for i := 0; i <= MaxRetries; i++ {
		dbCtx, cancel := context.WithTimeout(ctx, s.writeTimeout)
		start := time.Now()

		err = s.queries.InsertEventsBatch(dbCtx, db.InsertEventsBatchParams{
			ClickIds:     arrs.clickIDs,
			CampaignIds:  arrs.campaignIDs,
			UserIds:      arrs.userIDs,
			EventTypes:   arrs.eventTypes,
			Payloads:     arrs.payloads,
			IpAddresses:  arrs.ipAddresses,
			UserAgents:   arrs.userAgents,
			CreatedAt:    arrs.createdAts,
			CreatedDates: arrs.createdDates,
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
