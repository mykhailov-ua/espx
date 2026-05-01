package ads

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/repository"
	redis "github.com/redis/go-redis/v9"
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
// It uses Redis Streams for durable ingestion and a pool of workers for flushing to PostgreSQL.
type Processor struct {
	queries      repository.Querier
	rdb          redis.UniversalClient
	streamName   string
	groupName    string
	consumerID   string
	batchSize    int
	flushInt     time.Duration
	writeTimeout time.Duration
	maxWorkers   int
	wg           sync.WaitGroup
	cancel       context.CancelFunc
	drainOnce    sync.Once
}

// ErrBufferFull is kept for backward compatibility in tests, though Redis Streams don't "fill up" in the same way.
var ErrBufferFull = errors.New("event buffer is full")

// NewProcessor initializes a new event processor backed by Redis Streams.
func NewProcessor(
	queries repository.Querier,
	rdb redis.UniversalClient,
	streamName, groupName, consumerID string,
	batchSize int,
	maxWorkers int,
	flushInt, writeTimeout time.Duration,
) *Processor {
	return &Processor{
		queries:      queries,
		rdb:          rdb,
		streamName:   streamName,
		groupName:    groupName,
		consumerID:   consumerID,
		batchSize:    batchSize,
		flushInt:     flushInt,
		writeTimeout: writeTimeout,
		maxWorkers:   maxWorkers,
	}
}

// Process pushes an event into Redis Streams for durable processing.
func (p *Processor) Process(evt Event) error {
	if evt.ClickID == "" {
		id, err := uuid.NewV7()
		if err != nil {
			return err
		}
		evt.ClickID = id.String()
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.writeTimeout)
	defer cancel()

	_, err := p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: p.streamName,
		MaxLen: 100000,
		Approx: true,
		Values: map[string]interface{}{
			"click_id":    evt.ClickID,
			"campaign_id": evt.CampaignID.String(),
			"type":        evt.Type,
			"payload":     evt.Payload,
			"ip":          evt.IP,
			"ua":          evt.UA,
		},
	}).Result()

	if err != nil {
		EventsDropped.Inc()
		return err
	}

	EventsProcessed.Inc()
	return nil
}

// Start spawns background workers and the recovery janitor.
func (p *Processor) Start(ctx context.Context) {
	// Create a sub-context that we can cancel in Close()
	procCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	for i := 0; i < p.maxWorkers; i++ {
		p.wg.Add(1)
		go p.worker(procCtx)
	}

	p.wg.Add(1)
	go p.janitor(procCtx)
}

// Close signals workers to stop. This is used in tests and main shutdown.
func (p *Processor) Close() {
	if p.cancel != nil {
		p.cancel()
	}
}

// Wait blocks until all workers have finished processing their current batches.
func (p *Processor) Wait() {
	p.wg.Wait()
}

func (p *Processor) worker(ctx context.Context) {
	defer p.wg.Done()

	err := p.rdb.XGroupCreateMkStream(ctx, p.streamName, p.groupName, "$").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		slog.Error("failed to create consumer group", "error", err, "stream", p.streamName, "group", p.groupName)
		return
	}

	p.recoverPending(ctx)

	batch := make([]Event, 0, p.batchSize)
	msgIDs := make([]string, 0, p.batchSize)
	createdAts := make([]time.Time, 0, p.batchSize)
	ticker := time.NewTicker(p.flushInt)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				p.flushBatch(batch, msgIDs, createdAts)
			}
			p.drainOnce.Do(p.drainStream)
			return
		case <-ticker.C:
			if len(batch) > 0 {
				p.flushBatch(batch, msgIDs, createdAts)
				batch = batch[:0]
				msgIDs = msgIDs[:0]
				createdAts = createdAts[:0]
			}
		default:
			streams, err := p.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    p.groupName,
				Consumer: p.consumerID,
				Streams:  []string{p.streamName, ">"},
				Count:    int64(p.batchSize - len(batch)),
				Block:    p.flushInt,
			}).Result()

			if err != nil {
				if err != redis.Nil && ctx.Err() == nil {
					slog.Error("failed to read from redis stream", "error", err)
					time.Sleep(time.Second)
				}
				continue
			}

			for _, stream := range streams {
				for _, msg := range stream.Messages {
					evt := p.parseMessage(msg.Values)
					batch = append(batch, evt)
					msgIDs = append(msgIDs, msg.ID)
					createdAts = append(createdAts, p.extractTimestamp(msg.ID))

					if len(batch) >= p.batchSize {
						p.flushBatch(batch, msgIDs, createdAts)
						batch = batch[:0]
						msgIDs = msgIDs[:0]
						createdAts = createdAts[:0]
						ticker.Reset(p.flushInt)
					}
				}
			}
		}
	}
}

// drainStream reads and flushes all remaining unread messages from the Redis stream
// using a background context. Called during graceful shutdown to prevent data loss.
func (p *Processor) drainStream() {
	drainCtx, cancel := context.WithTimeout(context.Background(), p.writeTimeout)
	defer cancel()

	for {
		streams, err := p.rdb.XReadGroup(drainCtx, &redis.XReadGroupArgs{
			Group:    p.groupName,
			Consumer: p.consumerID,
			Streams:  []string{p.streamName, ">"},
			Count:    int64(p.batchSize),
			Block:    time.Millisecond,
		}).Result()

		if err != nil || len(streams) == 0 {
			break
		}

		for _, stream := range streams {
			if len(stream.Messages) == 0 {
				return
			}

			batch := make([]Event, 0, len(stream.Messages))
			msgIDs := make([]string, 0, len(stream.Messages))
			createdAts := make([]time.Time, 0, len(stream.Messages))

			for _, msg := range stream.Messages {
				batch = append(batch, p.parseMessage(msg.Values))
				msgIDs = append(msgIDs, msg.ID)
				createdAts = append(createdAts, p.extractTimestamp(msg.ID))
			}

			p.flushBatch(batch, msgIDs, createdAts)
			slog.Info("drained messages during shutdown", "count", len(batch))
		}
	}
}

func (p *Processor) parseMessage(values map[string]interface{}) Event {
	evt := Event{}
	if v, ok := values["click_id"].(string); ok {
		evt.ClickID = v
	}
	if v, ok := values["campaign_id"].(string); ok {
		evt.CampaignID, _ = uuid.Parse(v)
	}
	if v, ok := values["type"].(string); ok {
		evt.Type = v
	}
	if v, ok := values["payload"].(string); ok {
		evt.Payload = []byte(v)
	}
	if v, ok := values["ip"].(string); ok {
		evt.IP = v
	}
	if v, ok := values["ua"].(string); ok {
		evt.UA = v
	}
	return evt
}

func (p *Processor) flushBatch(batch []Event, msgIDs []string, createdAts []time.Time) {
	if len(batch) == 0 {
		return
	}

	p.flush(batch, createdAts)

	ctx, cancel := context.WithTimeout(context.Background(), p.writeTimeout)
	defer cancel()
	if err := p.rdb.XAck(ctx, p.streamName, p.groupName, msgIDs...).Err(); err != nil {
		slog.Error("failed to ack messages", "error", err, "stream", p.streamName, "group", p.groupName)
	}
}

func (p *Processor) extractTimestamp(id string) time.Time {
	parts := strings.Split(id, "-")
	if len(parts) < 1 {
		return time.Now()
	}
	msec, _ := strconv.ParseInt(parts[0], 10, 64)
	return time.Unix(msec/1000, (msec%1000)*1000000)
}

func (p *Processor) recoverPending(ctx context.Context) {
	for {
		entries, err := p.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    p.groupName,
			Consumer: p.consumerID,
			Streams:  []string{p.streamName, "0"},
			Count:    int64(p.batchSize),
		}).Result()

		if err != nil || len(entries) == 0 || len(entries[0].Messages) == 0 {
			return
		}

		batch := make([]Event, 0, len(entries[0].Messages))
		msgIDs := make([]string, 0, len(entries[0].Messages))
		createdAts := make([]time.Time, 0, len(entries[0].Messages))

		for _, msg := range entries[0].Messages {
			batch = append(batch, p.parseMessage(msg.Values))
			msgIDs = append(msgIDs, msg.ID)
			createdAts = append(createdAts, p.extractTimestamp(msg.ID))
		}

		p.flushBatch(batch, msgIDs, createdAts)
	}
}

func (p *Processor) janitor(ctx context.Context) {
	defer p.wg.Done()
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.claimStuckMessages(ctx)
		}
	}
}

func (p *Processor) claimStuckMessages(ctx context.Context) {
	startID := "0-0"
	for {
		entries, nextID, err := p.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   p.streamName,
			Group:    p.groupName,
			Consumer: p.consumerID,
			MinIdle:  5 * time.Minute,
			Start:    startID,
			Count:    int64(p.batchSize),
		}).Result()

		if err != nil {
			if err != redis.Nil {
				slog.Error("autoclaim failed", "error", err)
			}
			return
		}

		if len(entries) > 0 {
			batch := make([]Event, 0, len(entries))
			msgIDs := make([]string, 0, len(entries))
			createdAts := make([]time.Time, 0, len(entries))

			for _, msg := range entries {
				batch = append(batch, p.parseMessage(msg.Values))
				msgIDs = append(msgIDs, msg.ID)
				createdAts = append(createdAts, p.extractTimestamp(msg.ID))
			}
			p.flushBatch(batch, msgIDs, createdAts)
			slog.Info("successfully reclaimed and processed stuck messages", "count", len(entries))
		}

		if nextID == "0-0" {
			break
		}
		startID = nextID
	}
}

func (p *Processor) flush(batch []Event, createdAts []time.Time) {
	if len(batch) == 0 {
		return
	}

	clickIDs := make([]string, len(batch))
	campaignIDs := make([]pgtype.UUID, len(batch))
	eventTypes := make([]string, len(batch))
	payloads := make([][]byte, len(batch))
	ipAddresses := make([]string, len(batch))
	userAgents := make([]string, len(batch))
	dbCreatedAts := make([]pgtype.Timestamptz, len(batch))

	defaultPayload := []byte("{}")
	for i, evt := range batch {
		clickIDs[i] = evt.ClickID
		campaignIDs[i] = pgtype.UUID{Bytes: evt.CampaignID, Valid: true}
		eventTypes[i] = evt.Type
		if evt.Payload == nil {
			payloads[i] = defaultPayload
		} else {
			payloads[i] = evt.Payload
		}
		ipAddresses[i] = evt.IP
		userAgents[i] = evt.UA
		dbCreatedAts[i] = pgtype.Timestamptz{Time: createdAts[i], Valid: true}
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
			CreatedAt:   dbCreatedAts,
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
