// Package ads provides ClickHouseStore, which fans events into four ClickHouse tables
// (impressions, clicks, conversions, fraud_events) in a single insertToClickHouse call.
// Four pooled *[]*domain.Event slices are used to classify events by type before
// constructing PreparedBatch objects; the pool enforces a 5 000-element capacity cap
// to prevent unbounded memory retention on large flush batches. Events with the
// InsertedToCH flag set are skipped during the classification pass to ensure
// exactly-once insertion across StreamConsumer retries.
//
// Write failures are retried up to MaxRetries times with exponential back-off
// (InitialWait, MaxWait from processor.go). A metrics.DbWriteErrors counter and
// metrics.DbWriteDuration histogram are updated after each batch attempt.
package ads

import (
	"context"
	"fmt"
	"sync"
	"time"

	"espx/internal/domain"
	"espx/internal/metrics"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var slicePool = sync.Pool{
	New: func() any {
		s := make([]*domain.Event, 0, 1000)
		return &s
	},
}

// ClickHouseStore writes ad events to ClickHouse via the native protocol driver.
// writeTimeout bounds each PrepareBatch+Send round-trip; it must be set at least
// as high as the maximum network round-trip under full load to avoid spurious
// context cancellation errors that would incorrectly trigger the circuit breaker.
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

// StoreBatch persists a slice of events to ClickHouse with exponential retry.
// Events already marked InsertedToCH are skipped within insertToClickHouse.
// A DbWriteErrors counter is incremented after all retries are exhausted.
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
		for i := range *pImps {
			(*pImps)[i] = nil
		}
		*pImps = (*pImps)[:0]

		for i := range *pClicks {
			(*pClicks)[i] = nil
		}
		*pClicks = (*pClicks)[:0]

		for i := range *pConvs {
			(*pConvs)[i] = nil
		}
		*pConvs = (*pConvs)[:0]

		for i := range *pFraud {
			(*pFraud)[i] = nil
		}
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
		if e.InsertedToCH {
			continue
		}
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
					unsafeString(e.Payload),
					e.FraudReason,
					e.CreatedAt,
				)
			} else {
				err = batch.Append(
					e.ClickID,
					e.CampaignID,
					e.IP,
					e.UA,
					unsafeString(e.Payload),
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
		for _, e := range evts {
			e.InsertedToCH = true
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
