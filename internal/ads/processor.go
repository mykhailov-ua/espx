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
	redis "github.com/redis/go-redis/v9"
)

type Event struct {
	ClickID    string
	CampaignID uuid.UUID
	Type       string
	Payload    []byte
	IP         string
	UA         string
	CreatedAt  time.Time
}

// StreamConsumer implements event ingestion from Redis Streams to EventStore.
type StreamConsumer struct {
	store        EventStore
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
	recoverOnce  sync.Once
}

func NewStreamConsumer(
	store EventStore,
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

func (p *StreamConsumer) Process(evt Event) error {
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
	procCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	for i := 0; i < p.maxWorkers; i++ {
		p.wg.Add(1)
		go p.worker(procCtx)
	}

	p.wg.Add(1)
	go p.janitor(procCtx)
}

func (p *StreamConsumer) Close() {
	if p.cancel != nil {
		p.cancel()
	}
}

func (p *StreamConsumer) Wait() {
	p.wg.Wait()
}

func (p *StreamConsumer) worker(ctx context.Context) {
	defer p.wg.Done()

	err := p.rdb.XGroupCreateMkStream(ctx, p.streamName, p.groupName, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		slog.Error("failed to create consumer group", "error", err, "stream", p.streamName, "group", p.groupName)
		return
	}

	p.recoverOnce.Do(func() {
		initCtx, cancel := context.WithTimeout(context.Background(), p.writeTimeout*2)
		defer cancel()
		p.recoverPending(initCtx)
	})

	batch := make([]Event, 0, p.batchSize)
	msgIDs := make([]string, 0, p.batchSize)
	ticker := time.NewTicker(p.flushInt)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				fCtx, cancel := context.WithTimeout(context.Background(), p.writeTimeout)
				if err := p.flushBatch(fCtx, batch, msgIDs); err != nil {
					slog.Error("final worker flush failed", "error", err, "group", p.groupName)
				}
				cancel()
			}
			p.drainOnce.Do(p.drainStream)
			return
		case <-ticker.C:
			if len(batch) > 0 {
				if err := p.flushBatch(ctx, batch, msgIDs); err == nil {
					batch = batch[:0]
					msgIDs = msgIDs[:0]
				}
			}
		default:
			readCount := int64(p.batchSize - len(batch))
			if readCount <= 0 {
				if err := p.flushBatch(ctx, batch, msgIDs); err == nil {
					batch = batch[:0]
					msgIDs = msgIDs[:0]
					ticker.Reset(p.flushInt)
				} else {
					time.Sleep(100 * time.Millisecond)
				}
				continue
			}

			streams, err := p.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    p.groupName,
				Consumer: p.consumerID,
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
							batch = batch[:0]
							msgIDs = msgIDs[:0]
							ticker.Reset(p.flushInt)
						}
					}
				}
			}
		}
	}
}

func (p *StreamConsumer) drainStream() {
	drainCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	slog.Info("starting drain", "group", p.groupName)

	for {
		p.recoverPending(drainCtx)

		streams, err := p.rdb.XReadGroup(drainCtx, &redis.XReadGroupArgs{
			Group:    p.groupName,
			Consumer: p.consumerID,
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
			batch := make([]Event, 0, len(stream.Messages))
			msgIDs := make([]string, 0, len(stream.Messages))
			for _, msg := range stream.Messages {
				batch = append(batch, p.parseMessage(msg.ID, msg.Values))
				msgIDs = append(msgIDs, msg.ID)
			}
			if err := p.flushBatch(drainCtx, batch, msgIDs); err != nil {
				slog.Error("drain flush failed", "error", err, "group", p.groupName)
			}
		}
	}
}

func (p *StreamConsumer) parseMessage(id string, values map[string]interface{}) Event {
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

	// Extract timestamp from Redis message ID (format: <ms>-<seq>)
	parts := strings.Split(id, "-")
	if len(parts) > 0 {
		ms, err := strconv.ParseInt(parts[0], 10, 64)
		if err == nil {
			evt.CreatedAt = time.Unix(0, ms*int64(time.Millisecond))
		}
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now()
	}

	return evt
}

func (p *StreamConsumer) flushBatch(ctx context.Context, batch []Event, msgIDs []string) error {
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

func (p *StreamConsumer) recoverPending(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
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

			for _, msg := range entries[0].Messages {
				batch = append(batch, p.parseMessage(msg.ID, msg.Values))
				msgIDs = append(msgIDs, msg.ID)
			}

			if err := p.flushBatch(ctx, batch, msgIDs); err != nil {
				if !errors.Is(err, context.Canceled) {
					slog.Error("recovery flush failed", "error", err, "group", p.groupName)
				}
				return
			}
		}
	}
}

func (p *StreamConsumer) janitor(ctx context.Context) {
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
			batch := make([]Event, 0, len(entries))
			msgIDs := make([]string, 0, len(entries))

			for _, msg := range entries {
				batch = append(batch, p.parseMessage(msg.ID, msg.Values))
				msgIDs = append(msgIDs, msg.ID)
			}
			_ = p.flushBatch(ctx, batch, msgIDs)
		}

		if nextID == "0-0" {
			break
		}
		startID = nextID
	}
}
