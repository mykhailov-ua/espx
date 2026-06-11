package ads

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"espx/internal/domain"
	"espx/internal/metrics"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var slicePool = sync.Pool{
	New: func() any {
		s := make([]*domain.Event, 0, 20000)
		return &s
	},
}

// writeTimeout must cover worst-case round-trip under load; too low spuriously trips the circuit breaker.
type ClickHouseStore struct {
	conn          driver.Conn
	writeTimeout  time.Duration
	batchSize     atomic.Int64
	flushInterval atomic.Int64
	eventChan     chan *domain.Event
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
}

func NewClickHouseStore(conn driver.Conn, writeTimeout time.Duration) *ClickHouseStore {
	ctx, cancel := context.WithCancel(context.Background())
	s := &ClickHouseStore{
		conn:         conn,
		writeTimeout: writeTimeout,
		eventChan:    make(chan *domain.Event, 1000000),
		ctx:          ctx,
		cancel:       cancel,
	}
	s.batchSize.Store(20000)
	s.flushInterval.Store(int64(5 * time.Second))

	s.wg.Add(1)
	go s.backgroundFlusher()

	return s
}

func (s *ClickHouseStore) getBatchSize() int {
	return int(s.batchSize.Load())
}

func (s *ClickHouseStore) getFlushInterval() time.Duration {
	return time.Duration(s.flushInterval.Load())
}

func (s *ClickHouseStore) SetBatching(size int, interval time.Duration) {
	s.batchSize.Store(int64(size))
	s.flushInterval.Store(int64(interval))
}

func (s *ClickHouseStore) StoreBatch(ctx context.Context, events []*domain.Event) error {
	if len(events) == 0 {
		return nil
	}

	if s.getBatchSize() <= 1 {
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

	for _, e := range events {
		select {
		case s.eventChan <- e:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (s *ClickHouseStore) backgroundFlusher() {
	defer s.wg.Done()

	interval := s.getFlushInterval()
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

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

		if cap(*pImps) <= 100000 {
			slicePool.Put(pImps)
		}
		if cap(*pClicks) <= 100000 {
			slicePool.Put(pClicks)
		}
		if cap(*pConvs) <= 100000 {
			slicePool.Put(pConvs)
		}
		if cap(*pFraud) <= 100000 {
			slicePool.Put(pFraud)
		}
	}()

	flushAll := func() {
		if len(*pImps) > 0 {
			s.flushTableWithRetry("impressions", *pImps, false)
			*pImps = (*pImps)[:0]
		}
		if len(*pClicks) > 0 {
			s.flushTableWithRetry("clicks", *pClicks, false)
			*pClicks = (*pClicks)[:0]
		}
		if len(*pConvs) > 0 {
			s.flushTableWithRetry("conversions", *pConvs, false)
			*pConvs = (*pConvs)[:0]
		}
		if len(*pFraud) > 0 {
			s.flushTableWithRetry("fraud_events", *pFraud, true)
			*pFraud = (*pFraud)[:0]
		}
	}

	for {
		select {
		case <-s.ctx.Done():
			for {
				select {
				case e := <-s.eventChan:
					if e.FraudReason != "" {
						*pFraud = append(*pFraud, e)
					} else {
						switch e.Type {
						case "impression":
							*pImps = append(*pImps, e)
						case "click":
							*pClicks = append(*pClicks, e)
						case "conversion":
							*pConvs = append(*pConvs, e)
						}
					}
				default:
					goto drained
				}
			}
		drained:
			flushAll()
			return

		case e := <-s.eventChan:
			if e.FraudReason != "" {
				*pFraud = append(*pFraud, e)
				if len(*pFraud) >= s.getBatchSize() {
					s.flushTableWithRetry("fraud_events", *pFraud, true)
					*pFraud = (*pFraud)[:0]
				}
			} else {
				switch e.Type {
				case "impression":
					*pImps = append(*pImps, e)
					if len(*pImps) >= s.getBatchSize() {
						s.flushTableWithRetry("impressions", *pImps, false)
						*pImps = (*pImps)[:0]
					}
				case "click":
					*pClicks = append(*pClicks, e)
					if len(*pClicks) >= s.getBatchSize() {
						s.flushTableWithRetry("clicks", *pClicks, false)
						*pClicks = (*pClicks)[:0]
					}
				case "conversion":
					*pConvs = append(*pConvs, e)
					if len(*pConvs) >= s.getBatchSize() {
						s.flushTableWithRetry("conversions", *pConvs, false)
						*pConvs = (*pConvs)[:0]
					}
				}
			}

		case <-ticker.C:
			flushAll()
		}
	}
}

func (s *ClickHouseStore) flushTableWithRetry(table string, evts []*domain.Event, isFraud bool) {
	if len(evts) == 0 {
		return
	}

	waitTime := InitialWait
	var err error

	for i := 0; i <= MaxRetries; i++ {
		ctx, cancel := context.WithTimeout(s.ctx, s.writeTimeout)
		err = s.insertTable(ctx, table, evts, isFraud)
		cancel()

		if err == nil {
			return
		}

		slog.Error("failed to flush table to ClickHouse, retrying...", "table", table, "attempt", i, "error", err)

		if i < MaxRetries {
			timer := time.NewTimer(waitTime)
			select {
			case <-s.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
				waitTime *= 2
				if waitTime > MaxWait {
					waitTime = MaxWait
				}
			}
		}
	}

	slog.Error("failed to flush table to ClickHouse after all retries", "table", table, "error", err)
	metrics.DbWriteErrors.WithLabelValues("clickhouse").Inc()
}

func (s *ClickHouseStore) getDeduplicationToken(ctx context.Context, events []*domain.Event) string {
	if token, ok := ctx.Value(domain.DeduplicationTokenKey).(string); ok && token != "" {
		return token
	}
	if len(events) == 0 {
		return ""
	}
	h := sha256.New()
	for _, e := range events {
		h.Write([]byte(e.ClickID))
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(e.CreatedAt.UnixNano()))
		h.Write(buf[:])
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (s *ClickHouseStore) insertTable(ctx context.Context, table string, evts []*domain.Event, isFraud bool) error {
	start := time.Now()

	token := s.getDeduplicationToken(ctx, evts)
	query := fmt.Sprintf("INSERT INTO %s", table)
	if token != "" {
		query = fmt.Sprintf("INSERT INTO %s SETTINGS insert_deduplicate=1, insert_deduplication_token='%s'", table, token)
	}

	batch, err := s.conn.PrepareBatch(ctx, query)
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

	duration := time.Since(start).Seconds()
	metrics.DbWriteDuration.WithLabelValues("clickhouse").Observe(duration)

	return nil
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

		if cap(*pImps) <= 100000 {
			slicePool.Put(pImps)
		}
		if cap(*pClicks) <= 100000 {
			slicePool.Put(pClicks)
		}
		if cap(*pConvs) <= 100000 {
			slicePool.Put(pConvs)
		}
		if cap(*pFraud) <= 100000 {
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

		token := s.getDeduplicationToken(ctx, evts)
		query := fmt.Sprintf("INSERT INTO %s", table)
		if token != "" {
			query = fmt.Sprintf("INSERT INTO %s SETTINGS insert_deduplicate=1, insert_deduplication_token='%s'", table, token)
		}

		batch, err := s.conn.PrepareBatch(ctx, query)
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
	s.cancel()
	s.wg.Wait()
	return s.conn.Close()
}
