package ads

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/mykhailov-ua/ad-event-processor/internal/metrics"
)

var slicePool = sync.Pool{
	New: func() any {
		s := make([]*domain.Event, 0, 1000)
		return &s
	},
}

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

	metrics.DbWriteErrors.WithLabelValues("clickhouse").Inc()
	return err
}

func (s *ClickHouseStore) insertToClickHouse(ctx context.Context, events []*domain.Event) error {
	start := time.Now()

	pImps := slicePool.Get().(*[]*domain.Event)
	pClicks := slicePool.Get().(*[]*domain.Event)
	pConvs := slicePool.Get().(*[]*domain.Event)
	pFraud := slicePool.Get().(*[]*domain.Event)

	defer func() {
		*pImps = (*pImps)[:0]
		*pClicks = (*pClicks)[:0]
		*pConvs = (*pConvs)[:0]
		*pFraud = (*pFraud)[:0]
		if cap(*pImps) <= 5000 {
			slicePool.Put(pImps)
		}
		if cap(*pClicks) <= 5000 {
			slicePool.Put(pClicks)
		}
		if cap(*pConvs) <= 5000 {
			slicePool.Put(pConvs)
		}
		if cap(*pFraud) <= 5000 {
			slicePool.Put(pFraud)
		}
	}()

	imps := *pImps
	clicks := *pClicks
	convs := *pConvs
	fraud := *pFraud

	for i := range events {
		e := events[i]
		if e.FraudReason != "" {
			fraud = append(fraud, e)
			continue
		}

		switch e.Type {
		case "impression":
			imps = append(imps, e)
		case "click":
			clicks = append(clicks, e)
		case "conversion":
			convs = append(convs, e)
		}
	}

	*pImps, *pClicks, *pConvs, *pFraud = imps, clicks, convs, fraud

	insert := func(table string, evts []*domain.Event, isFraud bool) error {
		if len(evts) == 0 {
			return nil
		}

		batch, err := s.conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", table))
		if err != nil {
			return fmt.Errorf("prepare batch %s: %w", table, err)
		}

		for _, e := range evts {
			if isFraud {
				err = batch.Append(
					e.ClickID,
					e.CampaignID,
					e.UserID,
					e.Type,
					e.IP,
					e.UA,
					string(e.Payload),
					e.FraudReason,
					e.CreatedAt,
				)
			} else {
				err = batch.Append(
					e.ClickID,
					e.CampaignID,
					e.IP,
					e.UA,
					string(e.Payload),
					e.CreatedAt,
				)
			}
			if err != nil {
				return fmt.Errorf("append %s: %w", table, err)
			}
		}

		if err := batch.Send(); err != nil {
			return fmt.Errorf("send %s: %w", table, err)
		}
		return nil
	}

	if err := insert("impressions", imps, false); err != nil {
		return err
	}
	if err := insert("clicks", clicks, false); err != nil {
		return err
	}
	if err := insert("conversions", convs, false); err != nil {
		return err
	}
	if err := insert("fraud_events", fraud, true); err != nil {
		return err
	}

	duration := time.Since(start).Seconds()
	metrics.DbWriteDuration.WithLabelValues("clickhouse").Observe(duration)

	return nil
}

func (s *ClickHouseStore) Close() error {
	return s.conn.Close()
}
