package ads

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/repository"
)

// Event represents a single ad tracking event received from the transport layer.
type Event struct {
	ClickID    string    // ClickID is used for idempotency to prevent double-counting.
	CampaignID uuid.UUID // CampaignID identifies the target ad campaign.
	Type       string    // Type is one of: impression, click, conversion.
	Payload    []byte    // Payload contains raw JSON metadata for storage.
	IP         string    // IP address of the sender.
	UA         string    // User Agent of the sender.
}

// Processor handles asynchronous batching and persistence of ad events.
// It uses a pool of workers to achieve high throughput and database efficiency.
type Processor struct {
	queries      repository.Querier
	ch           chan Event
	batchSize    int
	flushInt     time.Duration
	writeTimeout time.Duration
	maxWorkers   int
	wg           sync.WaitGroup
}

// NewProcessor initializes a new event processor with a balanced buffer size.
func NewProcessor(queries repository.Querier, batchSize int, maxWorkers int, flushInt, writeTimeout time.Duration) *Processor {
	return &Processor{
		queries: queries,
		// Buffer size gives headroom for bursts during worker flushes.
		ch:           make(chan Event, batchSize*(maxWorkers+1)),
		batchSize:    batchSize,
		flushInt:     flushInt,
		writeTimeout: writeTimeout,
		maxWorkers:   maxWorkers,
	}
}

// ErrBufferFull is returned when the internal event channel is at capacity.
var ErrBufferFull = errors.New("event buffer is full")

// Process pushes an event into the processing pipeline.
// If the internal buffer is full, it returns ErrBufferFull to signal backpressure.
func (p *Processor) Process(evt Event) error {
	// Ensure ClickID exists for idempotency even if the client didn't provide one.
	if evt.ClickID == "" {
		id, err := uuid.NewV7()
		if err != nil {
			return err
		}
		evt.ClickID = id.String()
	}

	select {
	case p.ch <- evt:
		EventsProcessed.Inc()
		ProcessorBufferUsage.Set(float64(len(p.ch)))
		return nil
	default:
		EventsDropped.Inc()
		return ErrBufferFull
	}
}

// Start spawns background workers.
func (p *Processor) Start(ctx context.Context) {
	for i := 0; i < p.maxWorkers; i++ {
		p.wg.Add(1)
		go p.worker(ctx)
	}
}

// worker consumes events from the channel and flushes them in batches.
// It flushes either when the batch size is reached or the flush interval expires.
func (p *Processor) worker(ctx context.Context) {
	defer p.wg.Done()
	batch := make([]Event, 0, p.batchSize)
	ticker := time.NewTicker(p.flushInt)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Drain remaining events in the channel on context cancellation.
		drainLoop:
			for {
				select {
				case evt := <-p.ch:
					batch = append(batch, evt)
					if len(batch) >= p.batchSize {
						p.flush(batch)
						p.ClearBatch(&batch)
					}
				default:
					break drainLoop
				}
			}
			if len(batch) > 0 {
				p.flush(batch)
			}
			return
		case evt, ok := <-p.ch:
			if !ok {
				if len(batch) > 0 {
					p.flush(batch)
				}
				return
			}
			batch = append(batch, evt)
			if len(batch) >= p.batchSize {
				p.flush(batch)
				p.ClearBatch(&batch)
				ticker.Reset(p.flushInt)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				p.flush(batch)
				p.ClearBatch(&batch)
			}
		}
	}
}

// Close signals workers to drain and stop by closing the channel.
func (p *Processor) Close() {
	close(p.ch)
}

// Wait blocks until all workers have finished draining.
func (p *Processor) Wait() {
	p.wg.Wait()
}

// ClearBatch resets the batch slice while retaining the underlying array.
func (p *Processor) ClearBatch(batch *[]Event) {
	for i := range *batch {
		(*batch)[i].Payload = nil
		(*batch)[i].IP = ""
		(*batch)[i].UA = ""
	}
	*batch = (*batch)[:0]
}

// flush serializes the event batch and performs a bulk insert into PostgreSQL.
// It uses ON CONFLICT DO NOTHING for idempotency based on (click_id, created_at).
func (p *Processor) flush(batch []Event) {
	if len(batch) == 0 {
		return
	}

	clickIDs := make([]string, len(batch))
	campaignIDs := make([]pgtype.UUID, len(batch))
	eventTypes := make([]string, len(batch))
	payloads := make([][]byte, len(batch))
	ipAddresses := make([]string, len(batch))
	userAgents := make([]string, len(batch))
	createdAts := make([]pgtype.Timestamptz, len(batch))

	now := time.Now()
	for i, evt := range batch {
		clickIDs[i] = evt.ClickID
		campaignIDs[i] = pgtype.UUID{Bytes: evt.CampaignID, Valid: true}
		eventTypes[i] = evt.Type
		payloads[i] = evt.Payload
		ipAddresses[i] = evt.IP
		userAgents[i] = evt.UA
		createdAts[i] = pgtype.Timestamptz{Time: now, Valid: true}
	}

	var err error
	waitTime := InitialWait

	for i := 0; i <= MaxRetries; i++ {
		dbCtx, cancel := context.WithTimeout(context.Background(), p.writeTimeout)
		start := time.Now()

		err = p.queries.InsertEventsBatch(dbCtx, repository.InsertEventsBatchParams{
			ClickIds:    clickIDs,
			CampaignIds: campaignIDs,
			EventTypes:  eventTypes,
			Payloads:    payloads,
			IpAddresses: ipAddresses,
			UserAgents:  userAgents,
			CreatedAt:   createdAts,
		})

		duration := time.Since(start).Seconds()
		cancel()

		if err == nil {
			DbWriteDuration.WithLabelValues("batch_insert").Observe(duration)
			if i > 0 {
				slog.Info("successfully flushed event batch after retry", "attempts", i+1, "size", len(batch))
			}
			return
		}

		if i < MaxRetries {
			slog.Warn("failed to flush event batch, retrying...", "error", err, "attempt", i+1, "wait", waitTime, "size", len(batch))
			time.Sleep(waitTime)
			waitTime *= 2
			if waitTime > MaxWait {
				waitTime = MaxWait
			}
		}
	}

	DbWriteErrors.WithLabelValues("batch_insert").Inc()
	slog.Error("all retries failed for event batch, data lost", "error", err, "size", len(batch))
}
