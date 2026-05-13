package ads

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	redis "github.com/redis/go-redis/v9"
)

// StreamConsumer manages distributed event processing from Redis Streams.
// Chosen to provide a robust, concurrent worker pool with DLQ and circuit breaker support.
type StreamConsumer struct {
	store         domain.EventStore
	rdb           redis.UniversalClient
	streamName    string
	groupName     string
	consumerID    string
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	startMu       sync.Mutex
	flushInt      time.Duration
	writeTimeout  time.Duration
	retryInitWait time.Duration
	retryMaxWait  time.Duration
	streamMinIdle time.Duration
	drainTimeout  time.Duration
	batchSize     int
	maxWorkers    int
	maxRetries    int
	drainOnce     sync.Once
	started       bool
	cb            *CircuitBreaker
}

// NewStreamConsumer initializes the consumer with a unique ID and resiliency parameters.
func NewStreamConsumer(
	store domain.EventStore,
	rdb redis.UniversalClient,
	streamName, groupName, consumerID string,
	batchSize int,
	maxWorkers int,
	flushInt, writeTimeout time.Duration,
	retryInitWait, retryMaxWait time.Duration,
	maxRetries int,
	streamMinIdle time.Duration,
	drainTimeout time.Duration,
) *StreamConsumer {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	uniqueConsumerID := fmt.Sprintf("%s-%s-%s", consumerID, hostname, uuid.NewString()[:8])

	return &StreamConsumer{
		store:         store,
		rdb:           rdb,
		streamName:    streamName,
		groupName:     groupName,
		consumerID:    uniqueConsumerID,
		batchSize:     batchSize,
		flushInt:      flushInt,
		writeTimeout:  writeTimeout,
		maxWorkers:    maxWorkers,
		retryInitWait: retryInitWait,
		retryMaxWait:  retryMaxWait,
		maxRetries:    maxRetries,
		streamMinIdle: streamMinIdle,
		drainTimeout:  drainTimeout,
		cb:            NewCircuitBreaker(maxRetries, retryMaxWait*2),
	}
}

// Start spawns background workers, janitors, and DLQ monitors for the consumer group.
// Chosen to parallelize I/O operations while maintaining strict group-level message affinity.
func (p *StreamConsumer) Start(ctx context.Context) {
	p.startMu.Lock()
	defer p.startMu.Unlock()
	if p.started {
		return
	}
	p.started = true

	procCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	err := p.rdb.XGroupCreateMkStream(ctx, p.streamName, p.groupName, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		slog.Error("failed to create consumer group", "error", err, "stream", p.streamName, "group", p.groupName)
	}

	for i := 0; i < p.maxWorkers; i++ {
		p.wg.Add(1)
		go func(workerIdx int) {
			defer p.wg.Done()
			p.worker(procCtx, workerIdx)
		}(i)
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.janitor(procCtx)
	}()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.dlqMonitor(procCtx)
	}()
}

// Close initiates the shutdown sequence by canceling the internal context.
func (p *StreamConsumer) Close() {
	if p.cancel != nil {
		p.cancel()
	}
}

// Wait blocks until all background workers have completed their final flush and exited.
func (p *StreamConsumer) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *StreamConsumer) workerConsumerID(workerIdx int) string {
	return fmt.Sprintf("%s-w%d", p.consumerID, workerIdx)
}

// worker implements the primary message consumption loop with batching and retry logic.
// Chosen to ensure reliable message delivery even during transient persistence failures.
func (p *StreamConsumer) worker(ctx context.Context, workerIdx int) {
	workerID := p.workerConsumerID(workerIdx)
	defer func() {
		if r := recover(); r != nil {
			slog.Error("worker panic recovered", "error", r, "worker", workerID)
		}
	}()

	initCtx, initCancel := context.WithTimeout(context.Background(), p.writeTimeout*2)
	p.recoverPending(initCtx, workerID)
	initCancel()

	batch := make([]*domain.Event, 0, p.batchSize)
	msgIDs := make([]string, 0, p.batchSize)
	ticker := time.NewTicker(p.flushInt)
	defer ticker.Stop()

	retryWait := p.retryInitWait
	retryCount := 0

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				fCtx, fCancel := context.WithTimeout(context.Background(), p.writeTimeout)
				if err := p.flushBatch(fCtx, batch, msgIDs); err != nil {
					slog.Error("final worker flush failed", "error", err, "group", p.groupName, "worker", workerID)
				}
				for _, e := range batch {
					domain.EventPool.Put(e)
				}
				fCancel()
			}
			recoverCtx, recoverCancel := context.WithTimeout(context.Background(), p.writeTimeout)
			p.recoverPending(recoverCtx, workerID)
			recoverCancel()
			p.drainOnce.Do(func() { p.drainNewMessages(workerID) })
			return

		case <-ticker.C:
			if len(batch) > 0 {
				p.tryFlush(ctx, &batch, &msgIDs, &retryCount, workerID, ticker, &retryWait)
			}
		default:
			if !p.cb.Allow() {
				wait := p.cb.WaitDuration()
				if wait > 0 {
					slog.Warn("circuit breaker open, pausing reads",
						"group", p.groupName,
						"worker", workerID,
						"wait", wait,
					)
					select {
					case <-ctx.Done():
						continue
					case <-time.After(wait):
					}
				}
				continue
			}

			readCount := int64(p.batchSize - len(batch))
			if readCount <= 0 {
				p.tryFlush(ctx, &batch, &msgIDs, &retryCount, workerID, ticker, &retryWait)
				continue
			}

			streams, err := p.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    p.groupName,
				Consumer: workerID,
				Streams:  []string{p.streamName, ">"},
				Count:    readCount,
				Block:    p.flushInt,
			}).Result()

			if err != nil {
				if err != redis.Nil && !errors.Is(err, context.Canceled) {
					slog.Error("failed to read from redis stream", "error", err)
					time.Sleep(time.Second)
				}
				continue
			}

			for _, stream := range streams {
				for _, msg := range stream.Messages {
					batch = append(batch, p.parseMessage(msg.ID, msg.Values))
					msgIDs = append(msgIDs, msg.ID)

					if len(batch) >= p.batchSize {
						p.tryFlush(ctx, &batch, &msgIDs, &retryCount, workerID, ticker, &retryWait)
					}
				}
			}
		}
	}
}

// tryFlush attempts to persist a batch of events and updates the circuit breaker state.
func (p *StreamConsumer) tryFlush(ctx context.Context, batch *[]*domain.Event, msgIDs *[]string, retryCount *int, workerID string, ticker *time.Ticker, retryWait *time.Duration) {
	err := p.flushBatch(ctx, *batch, *msgIDs)
	if err == nil {
		p.cb.RecordSuccess()
		CircuitBreakerState.WithLabelValues(p.groupName).Set(float64(p.cb.State()))
		for _, e := range *batch {
			domain.EventPool.Put(e)
		}
		*batch = (*batch)[:0]
		*msgIDs = (*msgIDs)[:0]
		if ticker != nil {
			ticker.Reset(p.flushInt)
		}
		*retryWait = 100 * time.Millisecond
		*retryCount = 0
		return
	}

	*retryCount++
	p.cb.RecordFailure()
	CircuitBreakerState.WithLabelValues(p.groupName).Set(float64(p.cb.State()))

	if *retryCount > p.maxRetries {
		slog.Error("poison pill detected, moving to DLQ", "error", err, "group", p.groupName, "worker", workerID)
		pipe := p.rdb.Pipeline()

		for i, e := range *batch {
			pipe.XAdd(context.Background(), &redis.XAddArgs{
				Stream: "ad:events:dlq",
				Values: map[string]interface{}{
					"click_id":    e.ClickID,
					"campaign_id": e.CampaignID.String(),
					"type":        e.Type,
					"payload":     e.Payload,
					"ip":          e.IP,
					"ua":          e.UA,
					"error":       err.Error(),
					"original_id": (*msgIDs)[i],
					"failed_at":   time.Now().Format(time.RFC3339),
					"service":     "ad-event-processor",
					"worker_id":   workerID,
					"retry_count": *retryCount,
				},
			})
		}

		pipe.XAck(context.Background(), p.streamName, p.groupName, *msgIDs...)
		pipe.XDel(context.Background(), p.streamName, *msgIDs...)
		_, _ = pipe.Exec(context.Background())

		for _, e := range *batch {
			domain.EventPool.Put(e)
		}
		*batch = (*batch)[:0]
		*msgIDs = (*msgIDs)[:0]
		if ticker != nil {
			ticker.Reset(p.flushInt)
		}
		*retryWait = 100 * time.Millisecond
		*retryCount = 0
	} else {
		time.Sleep(*retryWait)
		*retryWait *= 2
		if *retryWait > p.retryMaxWait {
			*retryWait = p.retryMaxWait
		}
	}
}

// drainNewMessages fetches any remaining messages from the stream after context cancellation.
// Chosen to ensure zero data loss during graceful shutdown of consumer nodes.
func (p *StreamConsumer) drainNewMessages(workerID string) {
	drainCtx, cancel := context.WithTimeout(context.Background(), p.drainTimeout)
	defer cancel()

	slog.Info("starting drain", "group", p.groupName, "worker", workerID)

	for {
		streams, err := p.rdb.XReadGroup(drainCtx, &redis.XReadGroupArgs{
			Group:    p.groupName,
			Consumer: workerID,
			Streams:  []string{p.streamName, ">"},
			Count:    int64(p.batchSize),
			Block:    500 * time.Millisecond,
		}).Result()

		if err != nil {
			if err == redis.Nil {
				slog.Info("drain finished", "group", p.groupName)
				return
			}
			slog.Error("drain error", "error", err, "group", p.groupName)
			return
		}

		if len(streams) == 0 || len(streams[0].Messages) == 0 {
			slog.Info("drain finished", "group", p.groupName)
			return
		}

		for _, stream := range streams {
			batch := make([]*domain.Event, 0, len(stream.Messages))
			msgIDs := make([]string, 0, len(stream.Messages))
			for _, msg := range stream.Messages {
				batch = append(batch, p.parseMessage(msg.ID, msg.Values))
				msgIDs = append(msgIDs, msg.ID)
			}
			if err := p.flushBatch(drainCtx, batch, msgIDs); err != nil {
				slog.Error("drain flush failed", "error", err, "group", p.groupName)
			}
			for _, e := range batch {
				domain.EventPool.Put(e)
			}
		}
	}
}

// parseMessage converts a Redis Stream message into a domain event object.
func (p *StreamConsumer) parseMessage(id string, values map[string]interface{}) *domain.Event {
	evt := domain.EventPool.Get().(*domain.Event)
	evt.Reset()
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
		evt.Payload = append(evt.Payload[:0], v...)
	}
	if v, ok := values["ip"].(string); ok {
		evt.IP = v
	}
	if v, ok := values["ua"].(string); ok {
		evt.UA = v
	}

	if idx := strings.IndexByte(id, '-'); idx > 0 {
		ms, err := strconv.ParseInt(id[:idx], 10, 64)
		if err == nil {
			evt.CreatedAt = time.Unix(0, ms*int64(time.Millisecond))
		}
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now()
	}

	return evt
}

// flushBatch writes a collection of events to the persistent store and acknowledges them in Redis.
func (p *StreamConsumer) flushBatch(ctx context.Context, batch []*domain.Event, msgIDs []string) error {
	if len(batch) == 0 {
		return nil
	}

	err := p.store.StoreBatch(ctx, batch)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			slog.Error("store failed, NOT ACKING", "error", err, "group", p.groupName, "size", len(batch))
		}
		return err
	}

	ackCtx, cancel := context.WithTimeout(ctx, p.writeTimeout)
	defer cancel()
	if err := p.rdb.XAck(ackCtx, p.streamName, p.groupName, msgIDs...).Err(); err != nil {
		if !errors.Is(err, context.Canceled) {
			slog.Error("failed to ack", "error", err, "group", p.groupName)
		}
	}
	return nil
}

// recoverPending identifies and re-processes messages that were claimed but not acknowledged by this consumer.
func (p *StreamConsumer) recoverPending(ctx context.Context, consumerID string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			entries, err := p.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    p.groupName,
				Consumer: consumerID,
				Streams:  []string{p.streamName, "0"},
				Count:    int64(p.batchSize),
			}).Result()

			if err != nil || len(entries) == 0 || len(entries[0].Messages) == 0 {
				return
			}

			batch := make([]*domain.Event, 0, len(entries[0].Messages))
			msgIDs := make([]string, 0, len(entries[0].Messages))

			for _, msg := range entries[0].Messages {
				batch = append(batch, p.parseMessage(msg.ID, msg.Values))
				msgIDs = append(msgIDs, msg.ID)
			}

			if err := p.flushBatch(ctx, batch, msgIDs); err != nil {
				if !errors.Is(err, context.Canceled) {
					slog.Error("recovery flush failed", "error", err, "group", p.groupName)
				}
				for _, e := range batch {
					domain.EventPool.Put(e)
				}
				return
			}
			for _, e := range batch {
				domain.EventPool.Put(e)
			}
		}
	}
}

// janitor periodically scans for and claims messages that have been idle beyond the configured threshold.
func (p *StreamConsumer) janitor(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("janitor panic recovered", "error", r)
		}
	}()
	ticker := time.NewTicker(p.streamMinIdle)
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

// claimStuckMessages uses XAutoClaim to transition ownership of long-idle messages to this consumer.
func (p *StreamConsumer) claimStuckMessages(ctx context.Context) {
	startID := "0-0"
	for {
		entries, nextID, err := p.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   p.streamName,
			Group:    p.groupName,
			Consumer: p.consumerID,
			MinIdle:  p.streamMinIdle,
			Start:    startID,
			Count:    int64(p.batchSize),
		}).Result()

		if err != nil {
			if err != redis.Nil && !errors.Is(err, context.Canceled) {
				slog.Error("autoclaim failed", "error", err, "group", p.groupName)
			}
			return
		}

		if len(entries) > 0 {
			batch := make([]*domain.Event, 0, len(entries))
			msgIDs := make([]string, 0, len(entries))

			for _, msg := range entries {
				batch = append(batch, p.parseMessage(msg.ID, msg.Values))
				msgIDs = append(msgIDs, msg.ID)
			}
			_ = p.flushBatch(ctx, batch, msgIDs)
			for _, e := range batch {
				domain.EventPool.Put(e)
			}
		}

		if nextID == "0-0" {
			break
		}
		startID = nextID
	}
}

// dlqMonitor tracks the growth of the Dead Letter Queue to provide visibility into recurring processing failures.
func (p *StreamConsumer) dlqMonitor(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("dlq monitor panic recovered", "error", r)
		}
	}()
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			size, err := p.rdb.XLen(ctx, "ad:events:dlq").Result()
			if err != nil {
				if err != redis.Nil && !errors.Is(err, context.Canceled) {
					slog.Error("failed to get DLQ size", "error", err)
				}
				continue
			}
			DlqSize.Set(float64(size))
		}
	}
}
