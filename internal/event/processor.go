package event

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	maxRetries    = 3
	initialWait   = 100 * time.Millisecond
	maxWait       = 2 * time.Second
)

type Event struct {
	ID         uuid.UUID
	CampaignID uuid.UUID
	Type       string
	Payload    []byte
	IP         string
	UA         string
}

type Processor struct {
	pool      *pgxpool.Pool
	ch        chan Event
	batchSize int
	flushInt  time.Duration
	wg        sync.WaitGroup
}

func NewProcessor(pool *pgxpool.Pool, batchSize int, flushInt time.Duration) *Processor {
	return &Processor{
		pool:      pool,
		ch:        make(chan Event, batchSize*2),
		batchSize: batchSize,
		flushInt:  flushInt,
	}
}

var ErrBufferFull = errors.New("event buffer is full")

func (p *Processor) Process(evt Event) error {
	// Generate time-sorted UUIDv7
	id, err := uuid.NewV7()
	if err != nil {
		return err
	}
	evt.ID = id

	select {
	case p.ch <- evt:
		return nil
	default:
		return ErrBufferFull
	}
}

func (p *Processor) Start(ctx context.Context) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		batch := make([]Event, 0, p.batchSize)
		ticker := time.NewTicker(p.flushInt)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				// Drain the channel
			drainLoop:
				for {
					select {
					case evt := <-p.ch:
						batch = append(batch, evt)
						if len(batch) >= p.batchSize {
							p.flush(batch)
							p.clearBatch(&batch)
						}
					default:
						break drainLoop
					}
				}
				if len(batch) > 0 {
					p.flush(batch)
				}
				return
			case evt := <-p.ch:
				batch = append(batch, evt)
				if len(batch) >= p.batchSize {
					p.flush(batch)
					p.clearBatch(&batch)
					ticker.Reset(p.flushInt)
				}
			case <-ticker.C:
				if len(batch) > 0 {
					p.flush(batch)
					p.clearBatch(&batch)
				}
			}
		}
	}()
}

func (p *Processor) Wait() {
	p.wg.Wait()
}

func (p *Processor) clearBatch(batch *[]Event) {
	for i := range *batch {
		// Clear pointers to prevent memory leaks while reusing slice capacity
		(*batch)[i].Payload = nil
		(*batch)[i].IP = ""
		(*batch)[i].UA = ""
	}
	*batch = (*batch)[:0]
}

type eventCopySource struct {
	rows []Event
	idx  int
	now  time.Time
	row  []any
}

func (s *eventCopySource) Next() bool {
	s.idx++
	return s.idx < len(s.rows)
}

func (s *eventCopySource) Values() ([]any, error) {
	evt := &s.rows[s.idx]
	s.row[0] = pgtype.UUID{Bytes: evt.ID, Valid: true}
	s.row[1] = pgtype.UUID{Bytes: evt.CampaignID, Valid: true}
	s.row[2] = evt.Type
	s.row[3] = evt.Payload
	s.row[4] = evt.IP
	s.row[5] = evt.UA
	s.row[6] = s.now
	return s.row, nil
}

func (s *eventCopySource) Err() error {
	return nil
}

func (p *Processor) flush(batch []Event) {
	if len(batch) == 0 {
		return
	}

	var err error
	waitTime := initialWait

	for i := 0; i <= maxRetries; i++ {
		// Re-create source for each attempt to reset iterator
		source := &eventCopySource{
			rows: batch,
			idx:  -1,
			now:  time.Now(),
			row:  make([]any, 7),
		}

		_, err = p.pool.CopyFrom(
			context.Background(),
			pgx.Identifier{"events"},
			[]string{"id", "campaign_id", "event_type", "payload", "ip_address", "user_agent", "created_at"},
			source,
		)

		if err == nil {
			if i > 0 {
				slog.Info("successfully flushed event batch after retry", "attempts", i+1, "size", len(batch))
			}
			return
		}

		if i < maxRetries {
			slog.Warn("failed to flush event batch, retrying...",
				"error", err,
				"attempt", i+1,
				"wait", waitTime,
				"size", len(batch),
			)
			time.Sleep(waitTime)
			waitTime *= 2
			if waitTime > maxWait {
				waitTime = maxWait
			}
		}
	}

	slog.Error("all retries failed for event batch, data lost", "error", err, "size", len(batch))
}
