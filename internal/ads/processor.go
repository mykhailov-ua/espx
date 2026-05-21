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
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/pb"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/mykhailov-ua/ad-event-processor/internal/metrics"
	redis "github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

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
	started       bool
	cb            *CircuitBreaker
}

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

func (p *StreamConsumer) Close() {
	if p.cancel != nil {
		p.cancel()
	}
}

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

	retryWait := p.retryInitWait
	retryCount := 0
	lastFlush := time.Now()

	xreadArgs := &redis.XReadGroupArgs{
		Group:    p.groupName,
		Consumer: workerID,
		Streams:  []string{p.streamName, ">"},
		Block:    p.flushInt,
	}

	for {
		select {
		case <-ctx.Done():
			// Drain remaining events from the stream within drainTimeout to prevent data loss.
			drainCtx, drainCancel := context.WithTimeout(context.Background(), p.drainTimeout)
			if len(batch) > 0 {
				if err := p.flushBatch(drainCtx, batch, msgIDs); err != nil {
					slog.Error("drain flush of existing batch failed", "error", err, "group", p.groupName, "worker", workerID)
				}
				for _, e := range batch {
					domain.EventPool.Put(e)
				}
				batch = batch[:0]
				msgIDs = msgIDs[:0]
			}

			drainArgs := &redis.XReadGroupArgs{
				Group:    p.groupName,
				Consumer: workerID,
				Streams:  []string{p.streamName, ">"},
				Block:    -1,
			}

			for {
				if drainCtx.Err() != nil {
					break
				}

				readCount := int64(p.batchSize - len(batch))
				if readCount <= 0 {
					if err := p.flushBatch(drainCtx, batch, msgIDs); err != nil {
						slog.Error("drain batch flush failed", "error", err, "group", p.groupName, "worker", workerID)
						break
					}
					for _, e := range batch {
						domain.EventPool.Put(e)
					}
					batch = batch[:0]
					msgIDs = msgIDs[:0]
					readCount = int64(p.batchSize)
				}

				drainArgs.Count = readCount
				streams, err := p.rdb.XReadGroup(drainCtx, drainArgs).Result()
				if err != nil {
					if err == redis.Nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
						break
					}
					slog.Error("drain read failed", "error", err)
					break
				}

				hasNewMessages := false
				for _, stream := range streams {
					if len(stream.Messages) > 0 {
						hasNewMessages = true
					}
					for _, msg := range stream.Messages {
						batch = append(batch, p.parseMessage(msg.ID, msg.Values))
						msgIDs = append(msgIDs, msg.ID)
					}
				}

				if !hasNewMessages {
					break
				}
			}

			if len(batch) > 0 {
				if err := p.flushBatch(drainCtx, batch, msgIDs); err != nil {
					slog.Error("final drain flush failed", "error", err, "group", p.groupName, "worker", workerID)
				}
				for _, e := range batch {
					domain.EventPool.Put(e)
				}
			}

			p.recoverPending(drainCtx, workerID)
			drainCancel()
			return
		default:
		}

		readCount := int64(p.batchSize - len(batch))
		if readCount <= 0 {
			p.tryFlush(ctx, &batch, &msgIDs, &retryCount, workerID, nil, &retryWait)
			lastFlush = time.Now()
			continue
		}

		xreadArgs.Count = readCount
		streams, err := p.rdb.XReadGroup(ctx, xreadArgs).Result()

		if err != nil {
			if err == redis.Nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				if len(batch) > 0 && time.Since(lastFlush) >= p.flushInt {
					p.tryFlush(ctx, &batch, &msgIDs, &retryCount, workerID, nil, &retryWait)
					lastFlush = time.Now()
				}
			} else {
				slog.Error("failed to read from redis stream", "error", err)
				time.Sleep(time.Second)
			}
			continue
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				batch = append(batch, p.parseMessage(msg.ID, msg.Values))
				msgIDs = append(msgIDs, msg.ID)
			}
		}

		if len(batch) >= p.batchSize || time.Since(lastFlush) >= p.flushInt {
			p.tryFlush(ctx, &batch, &msgIDs, &retryCount, workerID, nil, &retryWait)
			lastFlush = time.Now()
		}
	}
}

func (p *StreamConsumer) tryFlush(ctx context.Context, batch *[]*domain.Event, msgIDs *[]string, retryCount *int, workerID string, ticker *time.Ticker, retryWait *time.Duration) {
	if !p.cb.Allow() {
		wait := p.cb.WaitDuration()
		if wait <= 0 {
			wait = 100 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		return
	}
	err := p.flushBatch(ctx, *batch, *msgIDs)
	if err == nil {
		p.cb.RecordSuccess(workerID)
		metrics.CircuitBreakerState.WithLabelValues(p.groupName).Set(float64(p.cb.State()))
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

	if errors.Is(err, context.Canceled) {
		return
	}

	*retryCount++
	p.cb.RecordFailure(workerID)
	metrics.CircuitBreakerState.WithLabelValues(p.groupName).Set(float64(p.cb.State()))

	if *retryCount > p.maxRetries {
		slog.Error("poison pill detected, decomposing batch", "error", err, "group", p.groupName, "worker", workerID)

		var failedIndices []int
		var successfulMsgIDs []string

		for i, e := range *batch {
			singleBatch := []*domain.Event{e}
			
			select {
			case <-ctx.Done():
				return
			default:
			}

			singleCtx, singleCancel := context.WithTimeout(ctx, p.writeTimeout)
			if singleErr := p.store.StoreBatch(singleCtx, singleBatch); singleErr != nil {
				singleCancel()
				failedIndices = append(failedIndices, i)
			} else {
				singleCancel()
				successfulMsgIDs = append(successfulMsgIDs, (*msgIDs)[i])
			}
		}

		if len(successfulMsgIDs) > 0 {
			ackCtx, ackCancel := context.WithTimeout(context.Background(), p.writeTimeout)
			_ = p.rdb.XAck(ackCtx, p.streamName, p.groupName, successfulMsgIDs...).Err()
			ackCancel()
		}

		if len(failedIndices) > 0 {
			failedBatch := make([]*domain.Event, 0, len(failedIndices))
			failedMsgIDs := make([]string, 0, len(failedIndices))
			for _, i := range failedIndices {
				failedBatch = append(failedBatch, (*batch)[i])
				failedMsgIDs = append(failedMsgIDs, (*msgIDs)[i])
			}

			execErr := p.moveToDLQ(ctx, failedBatch, failedMsgIDs, workerID, *retryCount, fmt.Errorf("batch decomposed: %w", err))

			if execErr != nil {
				slog.Error("failed to exec dlq pipeline, retaining in PEL", "error", execErr, "group", p.groupName)
				newBatch := (*batch)[:0]
				newMsgIDs := (*msgIDs)[:0]
				for _, i := range failedIndices {
					newBatch = append(newBatch, (*batch)[i])
					newMsgIDs = append(newMsgIDs, (*msgIDs)[i])
				}
				for i, e := range *batch {
					isFailed := false
					for _, fi := range failedIndices {
						if i == fi {
							isFailed = true
							break
						}
					}
					if !isFailed {
						domain.EventPool.Put(e)
					}
				}
				*batch = newBatch
				*msgIDs = newMsgIDs
				return
			}
		}

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



var dlqEventPool = sync.Pool{
	New: func() any {
		return new(pb.AdDLQEvent)
	},
}

func (p *StreamConsumer) moveToDLQ(ctx context.Context, batch []*domain.Event, msgIDs []string, workerID string, retryCount int, err error) error {
	pipe := p.rdb.Pipeline()
	errStr := err.Error()

	for i, e := range batch {
		pbDLQ := dlqEventPool.Get().(*pb.AdDLQEvent)
		if pbDLQ.OriginalEvent == nil {
			pbDLQ.OriginalEvent = new(pb.AdStreamEvent)
		} else {
			pbDLQ.OriginalEvent.Reset()
		}
		pbDLQ.Error = errStr
		pbDLQ.OriginalId = msgIDs[i]
		pbDLQ.FailedAtUnix = time.Now().Unix()
		pbDLQ.WorkerId = workerID
		pbDLQ.RetryCount = int32(retryCount)

		pbDLQ.OriginalEvent.ClickId = e.ClickID
		pbDLQ.OriginalEvent.CampaignId = e.CampaignID[:]
		pbDLQ.OriginalEvent.EventType = e.Type
		pbDLQ.OriginalEvent.Payload = e.Payload
		pbDLQ.OriginalEvent.Ip = e.IP
		pbDLQ.OriginalEvent.Ua = e.UA
		pbDLQ.OriginalEvent.CreatedAtUnix = e.CreatedAt.Unix()

		data, marshalErr := proto.Marshal(pbDLQ)
		dlqEventPool.Put(pbDLQ)

		if marshalErr != nil {
			slog.Error("failed to marshal DLQ event", "error", marshalErr)
			continue
		}

		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: "ad:events:dlq",
			MaxLen: 100000,
			Approx: true,
			Values: map[string]interface{}{
				"d": data,
			},
		})
		pipe.XAck(ctx, p.streamName, p.groupName, msgIDs[i])
		pipe.XDel(ctx, p.streamName, msgIDs[i])
	}

	execCtx, execCancel := context.WithTimeout(ctx, p.writeTimeout)
	_, execErr := pipe.Exec(execCtx)
	execCancel()
	return execErr
}

func (p *StreamConsumer) parseMessage(id string, values map[string]interface{}) *domain.Event {
	evt := domain.EventPool.Get().(*domain.Event)
	evt.Reset()

	if rawBytesStr, ok := values["d"].(string); ok {
		pbEvt := streamEventPool.Get().(*pb.AdStreamEvent)
		pbEvt.Reset()

		buf := unsafe.Slice(unsafe.StringData(rawBytesStr), len(rawBytesStr))
		if err := proto.Unmarshal(buf, pbEvt); err == nil {
			evt.ClickID = pbEvt.ClickId
			if len(pbEvt.CampaignId) == 16 {
				copy(evt.CampaignID[:], pbEvt.CampaignId)
			} else {
				evt.CampaignID, _ = uuid.Parse(string(pbEvt.CampaignId))
			}
			evt.Type = pbEvt.EventType
			evt.Payload = append(evt.Payload[:0], pbEvt.Payload...)
			evt.IP = pbEvt.Ip
			evt.UA = pbEvt.Ua
			if pbEvt.CreatedAtUnix > 0 {
				evt.CreatedAt = time.Unix(pbEvt.CreatedAtUnix, 0)
			}
		} else {
			slog.Error("failed to unmarshal stream event protobuf", "error", err)
		}
		streamEventPool.Put(pbEvt)
	} else {
		if v, ok := values["click_id"].(string); ok {
			evt.ClickID = v
		}
		if v, ok := values["campaign_id"].(string); ok {
			evt.CampaignID, _ = uuid.Parse(v)
		}
		if v, ok := values["user_id"].(string); ok {
			evt.UserID = v
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
		if v, ok := values["fraud_reason"].(string); ok {
			evt.FraudReason = v
		}
	}

	if evt.CreatedAt.IsZero() {
		if idx := strings.IndexByte(id, '-'); idx > 0 {
			ms, err := strconv.ParseInt(id[:idx], 10, 64)
			if err == nil {
				evt.CreatedAt = time.Unix(0, ms*int64(time.Millisecond))
			}
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
					p.cb.RecordFailure(consumerID)
					slog.Error("recovery flush failed, moving to DLQ", "error", err, "group", p.groupName)
					_ = p.moveToDLQ(ctx, batch, msgIDs, consumerID, 1, fmt.Errorf("recovery flush failed: %w", err))
				}
				for _, e := range batch {
					domain.EventPool.Put(e)
				}
				return
			}
			p.cb.RecordSuccess(consumerID)
			for _, e := range batch {
				domain.EventPool.Put(e)
			}
		}
	}
}

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
			var dlqBatch []*domain.Event
			var dlqMsgIDs []string

			for _, msg := range entries {
				evt := p.parseMessage(msg.ID, msg.Values)
				count, _ := p.rdb.HIncrBy(ctx, "ad:events:retries", msg.ID, 1).Result()
				if count > int64(p.maxRetries) {
					dlqBatch = append(dlqBatch, evt)
					dlqMsgIDs = append(dlqMsgIDs, msg.ID)
					_ = p.rdb.HDel(ctx, "ad:events:retries", msg.ID).Err()
				} else {
					batch = append(batch, evt)
					msgIDs = append(msgIDs, msg.ID)
				}
			}

			if len(dlqBatch) > 0 {
				slog.Error("autoclaim retry limit exceeded, moving to DLQ", "group", p.groupName, "count", len(dlqBatch))
				_ = p.moveToDLQ(ctx, dlqBatch, dlqMsgIDs, "janitor", p.maxRetries+1, errors.New("autoclaim delivery limit exceeded"))
				for _, e := range dlqBatch {
					domain.EventPool.Put(e)
				}
			}

			if len(batch) > 0 {
				if err := p.flushBatch(ctx, batch, msgIDs); err != nil {
					p.cb.RecordFailure("janitor")
					if !errors.Is(err, context.Canceled) {
						slog.Error("janitor flush failed", "error", err, "group", p.groupName)
					}
				} else {
					p.cb.RecordSuccess("janitor")
					for _, id := range msgIDs {
						_ = p.rdb.HDel(ctx, "ad:events:retries", id).Err()
					}
				}
				for _, e := range batch {
					domain.EventPool.Put(e)
				}
			}
		}

		if nextID == "0-0" {
			break
		}
		startID = nextID
	}
}

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
			metrics.DlqSize.Set(float64(size))
		}
	}
}
