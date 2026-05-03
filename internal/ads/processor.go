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
	"unsafe"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
)


type StreamConsumer struct {
	store        domain.EventStore
	rdb          redis.UniversalClient
	streamName   string
	groupName    string
	consumerID   string
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	batchSize    int
	flushInt     time.Duration
	writeTimeout time.Duration
	maxWorkers   int
	drainOnce    sync.Once
	started    bool
	startMu    sync.Mutex
}

func NewStreamConsumer(

	store domain.EventStore,
	rdb redis.UniversalClient,
	streamName, groupName, consumerID string,
	batchSize int,
	maxWorkers int,
	flushInt, writeTimeout time.Duration,
) *StreamConsumer {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	uniqueConsumerID := fmt.Sprintf("%s-%s-%s", consumerID, hostname, uuid.NewString()[:8])

	return &StreamConsumer{
		store:        store,
		rdb:          rdb,
		streamName:   streamName,
		groupName:    groupName,
		consumerID:   uniqueConsumerID,
		batchSize:    batchSize,
		flushInt:     flushInt,
		writeTimeout: writeTimeout,
		maxWorkers:   maxWorkers,
	}
}

func (p *StreamConsumer) Process(evt *domain.Event) error {
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

func (p *StreamConsumer) Start(ctx context.Context) {
	p.startMu.Lock()
	defer p.startMu.Unlock()
	if p.started {
		return
	}
	p.started = true

	procCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	// Ensure consumer group exists
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
}

func (p *StreamConsumer) Close() {
	if p.cancel != nil {
		p.cancel()
	}
}

func (p *StreamConsumer) Wait() {
	p.wg.Wait()
}

// workerConsumerID returns a unique consumer name for the given worker index.
// Each goroutine must have its own consumer identity to prevent PEL conflicts
// when multiple goroutines call XReadGroup concurrently.
func (p *StreamConsumer) workerConsumerID(workerIdx int) string {
	return fmt.Sprintf("%s-w%d", p.consumerID, workerIdx)
}

func (p *StreamConsumer) worker(ctx context.Context, workerIdx int) {
	workerID := p.workerConsumerID(workerIdx)

	// Each worker recovers its own PEL.
	initCtx, initCancel := context.WithTimeout(context.Background(), p.writeTimeout*2)
	p.recoverPending(initCtx, workerID)
	initCancel()

	batch := make([]*domain.Event, 0, p.batchSize)
	msgIDs := make([]string, 0, p.batchSize)
	ticker := time.NewTicker(p.flushInt)
	defer ticker.Stop()

	// Backoff state for flush retries when DB is unavailable.
	// Prevents a hot loop of 10 retries/sec per worker.
	retryWait := 100 * time.Millisecond
	const maxRetryWait = 5 * time.Second
	retryCount := 0
	const maxRetries = 5

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
			// Each worker recovers its own PEL independently.
			// sync.Once.Do blocks all callers until the function completes,
			// so by the time drainOnce runs, this worker's PEL is already clear.
			recoverCtx, recoverCancel := context.WithTimeout(context.Background(), p.writeTimeout)
			p.recoverPending(recoverCtx, workerID)
			recoverCancel()
			// One worker drains new unassigned messages from the stream.
			p.drainOnce.Do(func() { p.drainNewMessages(workerID) })
			return
		case <-ticker.C:
			if len(batch) > 0 {
				if err := p.flushBatch(ctx, batch, msgIDs); err == nil {
					for _, e := range batch {
						domain.EventPool.Put(e)
					}
					batch = batch[:0]
					msgIDs = msgIDs[:0]
					retryWait = 100 * time.Millisecond
				}
			}
		default:
			readCount := int64(p.batchSize - len(batch))
			if readCount <= 0 {
				if err := p.flushBatch(ctx, batch, msgIDs); err == nil {
					for _, e := range batch {
						domain.EventPool.Put(e)
					}
					batch = batch[:0]
					msgIDs = msgIDs[:0]
					ticker.Reset(p.flushInt)
					retryWait = 100 * time.Millisecond
					retryCount = 0
				} else {
					retryCount++
					if retryCount > maxRetries {
						slog.Error("poison pill detected, dropping batch", "error", err, "group", p.groupName, "worker", workerID)
						pipe := p.rdb.Pipeline()
						pipe.XAck(context.Background(), p.streamName, p.groupName, msgIDs...)
						pipe.XDel(context.Background(), p.streamName, msgIDs...)
						_, _ = pipe.Exec(context.Background())
						
						for _, e := range batch {
							domain.EventPool.Put(e)
						}
						batch = batch[:0]
						msgIDs = msgIDs[:0]
						ticker.Reset(p.flushInt)
						retryWait = 100 * time.Millisecond
						retryCount = 0
					} else {
						time.Sleep(retryWait)
						retryWait *= 2
						if retryWait > maxRetryWait {
							retryWait = maxRetryWait
						}
					}
				}
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
						if err := p.flushBatch(ctx, batch, msgIDs); err == nil {
							for _, e := range batch {
								domain.EventPool.Put(e)
							}
							batch = batch[:0]
							msgIDs = msgIDs[:0]
							ticker.Reset(p.flushInt)
							retryWait = 100 * time.Millisecond
						}
					}
				}
			}
		}
	}
}

func (p *StreamConsumer) drainNewMessages(workerID string) {
	drainCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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
		// Zero-copy string→[]byte. Safe: the source string from go-redis is
		// a heap-allocated copy, and downstream consumers (pgx, clickhouse-go)
		// only read the slice.
		evt.Payload = unsafe.Slice(unsafe.StringData(v), len(v))
	}
	if v, ok := values["ip"].(string); ok {
		evt.IP = v
	}
	if v, ok := values["ua"].(string); ok {
		evt.UA = v
	}

	// Extract timestamp from Redis message ID (format: <ms>-<seq>).
	// strings.IndexByte avoids the []string allocation from strings.Split.
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

// recoverPending re-processes messages stuck in the given consumer's PEL.
// For fresh consumer names this is a no-op.
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

func (p *StreamConsumer) janitor(ctx context.Context) {
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

// claimStuckMessages uses XAutoClaim to reclaim messages from dead consumers.
// Claimed messages are immediately processed and ACKed, so the consumer ID
// used here (p.consumerID) is a transient PEL holder — the entry is removed
// upon successful ACK.
func (p *StreamConsumer) claimStuckMessages(ctx context.Context) {
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
