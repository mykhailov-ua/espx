package ads

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
)

type ClickHouseStore struct {
	conn         driver.Conn
	writeTimeout time.Duration
}

func NewClickHouseStore(conn driver.Conn, writeTimeout time.Duration) *ClickHouseStore {
	return &ClickHouseStore{
		conn:         conn,
		writeTimeout: writeTimeout,
	}
}

func (s *ClickHouseStore) StoreBatch(ctx context.Context, events []*domain.Event) error {
	if len(events) == 0 {
		return nil
	}

	var err error
	waitTime := InitialWait

	for i := 0; i <= MaxRetries; i++ {
		dbCtx, cancel := context.WithTimeout(ctx, s.writeTimeout)
		err = s.insertToClickHouse(dbCtx, events)
		cancel()

		if err == nil {
			return nil
		}

		if i < MaxRetries {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(waitTime):
				waitTime *= 2
				if waitTime > MaxWait {
					waitTime = MaxWait
				}
			}
		}
	}

	DbWriteErrors.WithLabelValues("clickhouse").Inc()
	return err
}

func (s *ClickHouseStore) insertToClickHouse(ctx context.Context, events []*domain.Event) error {
	start := time.Now()

	imps := make([]*domain.Event, 0, len(events))
	clicks := make([]*domain.Event, 0, len(events))
	convs := make([]*domain.Event, 0, len(events))

	for i := range events {
		e := events[i]
		switch e.Type {
		case "impression":
			imps = append(imps, e)
		case "click":
			clicks = append(clicks, e)
		case "conversion":
			convs = append(convs, e)
		}
	}

	insert := func(table string, evts []*domain.Event) error {
		if len(evts) == 0 {
			return nil
		}

		batch, err := s.conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", table))
		if err != nil {
			return fmt.Errorf("prepare batch %s: %w", table, err)
		}

		for _, e := range evts {
			err := batch.Append(
				e.ClickID,
				e.CampaignID,
				e.IP,
				e.UA,
				string(e.Payload),
				e.CreatedAt,
			)
			if err != nil {
				return fmt.Errorf("append %s: %w", table, err)
			}
		}

		if err := batch.Send(); err != nil {
			return fmt.Errorf("send %s: %w", table, err)
		}
		return nil
	}

	if err := insert("impressions", imps); err != nil {
		return err
	}
	if err := insert("clicks", clicks); err != nil {
		return err
	}
	if err := insert("conversions", convs); err != nil {
		return err
	}

	duration := time.Since(start).Seconds()
	DbWriteDuration.WithLabelValues("clickhouse").Observe(duration)

	return nil
}

func (s *ClickHouseStore) Close() error {
	return s.conn.Close()
}
