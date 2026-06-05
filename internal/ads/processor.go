// Package ads implements the Redis Streams consumer pipeline that drains ad-event
// messages from sharded streams into PostgreSQL (transactional ledger) and ClickHouse
// (columnar telemetry). StreamConsumer operates a fan-out of worker goroutines each
// holding an XReadGroup lease; batch accumulation is bounded by batchSize and flushInt
// to amortize per-write PostgreSQL round-trips. Write failures trigger exponential
// back-off controlled by the embedded CircuitBreaker; repeated failures decompose the
// batch into singleton writes to isolate poison-pill messages before escalating
// survivors to the ad:events:dlq stream serialized as vtproto-encoded AdDLQEvent.
//
// The janitor goroutine uses XAutoClaim to reclaim messages abandoned by crashed
// consumers after streamMinIdle, preventing PEL (Pending Entry List) accumulation.
// The dlqMonitor goroutine samples XLen every 15 s to keep the DLQ Prometheus gauge
// current. All goroutines check ctx.Done() on every iteration; shutdown drains any
// buffered batch before returning. See docs/architecture.md Processor section.
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
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/pb"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/mykhailov-ua/ad-event-processor/internal/metrics"
	"github.com/mykhailov-ua/ad-event-processor/pkg/logger"
	redis "github.com/redis/go-redis/v9"
)

// StreamConsumer is a Redis Streams consumer group worker pool for a single logical
// stream. It maintains maxWorkers concurrent XReadGroup loops, a janitor goroutine
// for autoclaim, and a dlqMonitor goroutine for queue-depth observability. The
// CircuitBreaker field gates flush attempts; when Open, workers back off for the
// remaining timeout duration rather than hammering a degraded downstream.
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
	logger        *logger.Logger
}

func (p *StreamConsumer) SetLogger(l *logger.Logger) {
	p.logger = l
}

var logBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 512)
		return &b
	},
}

var adLogRecordPool = sync.Pool{
	New: func() any {
		return &pb.AdLogRecord{}
	},
}

// NewStreamConsumer constructs a StreamConsumer and derives a globally unique
// consumerID by combining the provided base ID, the OS hostname, and a random UUID
// suffix to prevent PEL conflicts when multiple processor replicas share the same
// configuration. The CircuitBreaker is initialized with a timeout of 2x retryMaxWait.
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

// Start creates the consumer group if it does not exist (XGroupCreateMkStream with
// offset "0" to process all historical messages on first boot), then launches
// maxWorkers worker goroutines plus one janitor and one dlqMonitor. It is idempotent:
// concurrent calls are serialized by startMu and subsequent calls are no-ops.
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

// Wait blocks until all internal goroutines exit or the supplied context expires.
// Use after Close to guarantee a clean drain before the process terminates.
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
			slog.Error("worker panic recovered - exiting process", "error", r, "worker", workerID)
			os.Exit(1)
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
	}

	for {
		select {
		case <-ctx.Done():
			drainCtx, drainCancel := context.WithTimeout(context.Background(), p.drainTimeout)
			if len(batch) > 0 {
				if err := p.flushBatch(drainCtx, batch, msgIDs, workerID); err == nil {
					for _, e := range batch {
						domain.EventPool.Put(e)
					}
				} else {
					slog.Error("drain flush of existing batch failed, GC will reclaim objects", "error", err, "group", p.groupName, "worker", workerID)
				}
				batch = batch[:0]
				msgIDs = msgIDs[:0]
			}

			p.drainNewMessages(drainCtx, workerID)
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

		var blockTime time.Duration
		if len(batch) == 0 {
			blockTime = 200 * time.Millisecond
		} else {
			elapsed := time.Since(lastFlush)
			if elapsed >= p.flushInt {
				p.tryFlush(ctx, &batch, &msgIDs, &retryCount, workerID, nil, &retryWait)
				lastFlush = time.Now()
				continue
			}
			blockTime = p.flushInt - elapsed
			if blockTime > 200*time.Millisecond {
				blockTime = 200 * time.Millisecond
			}
		}

		xreadArgs.Count = readCount
		xreadArgs.Block = blockTime
		streams, err := p.rdb.XReadGroup(ctx, xreadArgs).Result()

		if err != nil {
			if err == redis.Nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				if len(batch) > 0 && time.Since(lastFlush) >= p.flushInt {
					p.tryFlush(ctx, &batch, &msgIDs, &retryCount, workerID, nil, &retryWait)
					lastFlush = time.Now()
				}
			} else {
				slog.Error("failed to read from redis stream", "error", err)
				select {
				case <-ctx.Done():
				case <-time.After(time.Second):
				}
			}
			continue
		}

		hadEmptyBatch := len(batch) == 0

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				batch = append(batch, p.parseMessage(msg.ID, msg.Values))
				msgIDs = append(msgIDs, msg.ID)
			}
		}

		if hadEmptyBatch && len(batch) > 0 {
			lastFlush = time.Now()
		}

		if len(batch) >= p.batchSize || time.Since(lastFlush) >= p.flushInt {
			p.tryFlush(ctx, &batch, &msgIDs, &retryCount, workerID, nil, &retryWait)
			lastFlush = time.Now()
		}
	}
}

func (p *StreamConsumer) recordSuccess(workerID string) {
	p.cb.RecordSuccess(workerID)
	metrics.CircuitBreakerState.WithLabelValues(p.groupName).Set(float64(p.cb.State()))
}

func (p *StreamConsumer) recordFailure(workerID string) {
	p.cb.RecordFailure(workerID)
	metrics.CircuitBreakerState.WithLabelValues(p.groupName).Set(float64(p.cb.State()))
}

func (p *StreamConsumer) recordCancellation(workerID string) {
	p.cb.RecordCancellation(workerID)
	metrics.CircuitBreakerState.WithLabelValues(p.groupName).Set(float64(p.cb.State()))
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
	err := p.flushBatch(ctx, *batch, *msgIDs, workerID)
	if err == nil {
		p.recordSuccess(workerID)
		_ = p.rdb.HDel(ctx, "ad:events:retries", (*msgIDs)...).Err()
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
		p.recordCancellation(workerID)
		return
	}

	*retryCount++
	p.recordFailure(workerID)

	pipe := p.rdb.Pipeline()
	incrCmds := make([]*redis.IntCmd, len(*msgIDs))
	for i, id := range *msgIDs {
		incrCmds[i] = pipe.HIncrBy(ctx, "ad:events:retries", id, 1)
	}
	_, _ = pipe.Exec(ctx)

	hasPoisonPill := false
	maxIncr := int64(0)
	for i := range *msgIDs {
		cVal, _ := incrCmds[i].Result()
		if cVal > maxIncr {
			maxIncr = cVal
		}
		if cVal > int64(p.maxRetries) {
			hasPoisonPill = true
		}
	}

	if maxIncr > int64(*retryCount) {
		*retryCount = int(maxIncr)
	}

	if hasPoisonPill {
		slog.Error("poison pill detected, decomposing batch", "error", err, "group", p.groupName, "worker", workerID)

		failedIndices := make([]int, 0, len(*batch))
		successfulMsgIDs := make([]string, 0, len(*batch))
		singleBatch := make([]*domain.Event, 1)

		for i, e := range *batch {
			if ctx.Err() != nil {
				for j := i; j < len(*batch); j++ {
					failedIndices = append(failedIndices, j)
				}
				break
			}

			singleBatch[0] = e
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
			_ = p.rdb.HDel(ackCtx, "ad:events:retries", successfulMsgIDs...).Err()
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
				fiIdx := 0
				for i, e := range *batch {
					if fiIdx < len(failedIndices) && i == failedIndices[fiIdx] {
						fiIdx++
					} else {
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
		select {
		case <-ctx.Done():
			return
		case <-time.After(*retryWait):
		}
		*retryWait *= 2
		if *retryWait > p.retryMaxWait {
			*retryWait = p.retryMaxWait
		}
	}
}

var (
	dlqEventPool = sync.Pool{
		New: func() any {
			return new(pb.AdDLQEvent)
		},
	}
	dlqValuesPool = sync.Pool{
		New: func() any {
			slice := make([]any, 2)
			slice[0] = "d"
			return &slice
		},
	}
)

func (p *StreamConsumer) moveToDLQ(ctx context.Context, batch []*domain.Event, msgIDs []string, workerID string, retryCount int, err error) error {
	errStr := err.Error()

	pipeWrite := p.rdb.Pipeline()

	writtenMsgIDs := make([]string, 0, len(batch))
	valuesPtrs := make([]*[]any, 0, len(batch))
	bufPtrs := make([]*[]byte, 0, len(batch))
	wrapPtrs := make([]*ByteSliceValue, 0, len(batch))
	defer func() {
		for _, ptr := range valuesPtrs {
			dlqValuesPool.Put(ptr)
		}
		for _, ptr := range bufPtrs {
			byteBufPool.Put(ptr)
		}
		for _, ptr := range wrapPtrs {
			byteSliceValuePool.Put(ptr)
		}
	}()

	execCtx, execCancel := context.WithTimeout(context.Background(), p.writeTimeout)
	defer execCancel()

	for i, e := range batch {
		pbDLQ := dlqEventPool.Get().(*pb.AdDLQEvent)
		if pbDLQ.OriginalEvent == nil {
			pbDLQ.OriginalEvent = new(pb.AdStreamEvent)
		} else {
			DeepResetAdStreamEvent(pbDLQ.OriginalEvent)
		}
		pbDLQ.Error = append(pbDLQ.Error[:0], errStr...)
		pbDLQ.OriginalId = append(pbDLQ.OriginalId[:0], msgIDs[i]...)
		pbDLQ.FailedAtUnix = time.Now().Unix()
		pbDLQ.WorkerId = append(pbDLQ.WorkerId[:0], workerID...)
		pbDLQ.RetryCount = int32(retryCount)

		pbDLQ.OriginalEvent.ClickId = append(pbDLQ.OriginalEvent.ClickId[:0], e.ClickID...)
		pbDLQ.OriginalEvent.CampaignId = append(pbDLQ.OriginalEvent.CampaignId[:0], e.CampaignID[:]...)
		pbDLQ.OriginalEvent.EventType = append(pbDLQ.OriginalEvent.EventType[:0], e.Type...)
		pbDLQ.OriginalEvent.Payload = append(pbDLQ.OriginalEvent.Payload[:0], e.Payload...)
		pbDLQ.OriginalEvent.Ip = append(pbDLQ.OriginalEvent.Ip[:0], e.IP...)
		pbDLQ.OriginalEvent.Ua = append(pbDLQ.OriginalEvent.Ua[:0], e.UA...)
		pbDLQ.OriginalEvent.CreatedAtUnix = e.CreatedAt.Unix()

		size := pbDLQ.SizeVT()
		bufPtr := byteBufPool.Get().(*[]byte)
		buf := *bufPtr
		if cap(buf) < size {
			buf = make([]byte, size)
		} else {
			buf = buf[:size]
		}

		n, marshalErr := pbDLQ.MarshalToSizedBufferVT(buf)
		if marshalErr != nil {
			slog.Error("failed to marshal DLQ event", "error", marshalErr)
			DeepResetAdDLQEvent(pbDLQ)
			dlqEventPool.Put(pbDLQ)
			*bufPtr = buf
			byteBufPool.Put(bufPtr)
			continue
		}

		data := buf[:n]
		*bufPtr = buf
		bufPtrs = append(bufPtrs, bufPtr)

		DeepResetAdDLQEvent(pbDLQ)
		dlqEventPool.Put(pbDLQ)
		writtenMsgIDs = append(writtenMsgIDs, msgIDs[i])

		valuesPtr := dlqValuesPool.Get().(*[]any)
		values := *valuesPtr

		wrap := byteSliceValuePool.Get().(*ByteSliceValue)
		wrap.b = data
		values[1] = wrap
		wrapPtrs = append(wrapPtrs, wrap)

		valuesPtrs = append(valuesPtrs, valuesPtr)

		pipeWrite.XAdd(execCtx, &redis.XAddArgs{
			Stream: "ad:events:dlq",
			MaxLen: 100000,
			Approx: true,
			Values: values,
		})
	}

	if len(writtenMsgIDs) == 0 {
		return nil
	}

	cmders, execErr := pipeWrite.Exec(execCtx)

	var hasError bool
	if execErr != nil && !errors.Is(execErr, redis.Nil) {
		slog.Error("DLQ write pipeline returned error", "error", execErr)
		hasError = true
	}

	pipeAck := p.rdb.Pipeline()
	ackedMsgIDs := make([]string, 0, len(batch))

	ackCtx, ackCancel := context.WithTimeout(context.Background(), p.writeTimeout)
	defer ackCancel()

	for i, cmder := range cmders {
		if cmder.Err() == nil {
			msgID := writtenMsgIDs[i]
			pipeAck.XAck(ackCtx, p.streamName, p.groupName, msgID)
			pipeAck.XDel(ackCtx, p.streamName, msgID)
			ackedMsgIDs = append(ackedMsgIDs, msgID)
		} else {
			slog.Error("individual DLQ write failed", "error", cmder.Err(), "msgID", writtenMsgIDs[i])
			hasError = true
		}
	}

	if len(ackedMsgIDs) > 0 {
		_, ackErr := pipeAck.Exec(ackCtx)
		if ackErr != nil {
			slog.Error("DLQ ack/del pipeline failed", "error", ackErr)
			return ackErr
		}
	}

	if hasError || len(ackedMsgIDs) < len(writtenMsgIDs) {
		return fmt.Errorf("DLQ write partial failure: wrote %d of %d messages", len(ackedMsgIDs), len(writtenMsgIDs))
	}

	return nil
}

func (p *StreamConsumer) parseMessage(id string, values map[string]interface{}) *domain.Event {
	evt := domain.EventPool.Get().(*domain.Event)
	evt.Reset()

	if rawBytesStr, ok := values["d"].(string); ok {
		pbEvt := streamEventPool.Get().(*pb.AdStreamEvent)
		DeepResetAdStreamEvent(pbEvt)

		buf := UnsafeBytes(rawBytesStr)
		if err := pbEvt.UnmarshalVT(buf); err == nil {
			evt.ClickID = unsafeString(pbEvt.ClickId)
			if len(pbEvt.CampaignId) == 16 {
				copy(evt.CampaignID[:], pbEvt.CampaignId)
			} else {
				evt.CampaignID, _ = uuid.ParseBytes(pbEvt.CampaignId)
			}
			evt.Type = unsafeString(pbEvt.EventType)
			evt.Payload = append(evt.Payload[:0], pbEvt.Payload...)
			evt.IP = unsafeString(pbEvt.Ip)
			evt.UA = unsafeString(pbEvt.Ua)
			if pbEvt.CreatedAtUnix > 0 {
				evt.CreatedAt = time.Unix(pbEvt.CreatedAtUnix, 0)
			}
		} else {
			slog.Error("failed to unmarshal stream event protobuf", "error", err)
		}
		DeepResetAdStreamEvent(pbEvt)
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

func firstN(ids []string, n int) []string {
	if len(ids) <= n {
		return ids
	}
	return ids[:n]
}

func (p *StreamConsumer) flushBatch(ctx context.Context, batch []*domain.Event, msgIDs []string, workerID string) error {
	if len(batch) == 0 {
		return nil
	}

	slog.Debug("flushing batch", "group", p.groupName, "batch_size", len(batch), "first_ids", firstN(msgIDs, 5))

	storeCtx, storeCancel := context.WithTimeout(ctx, p.writeTimeout)
	defer storeCancel()

	err := p.store.StoreBatch(storeCtx, batch)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			slog.Error("store failed, NOT ACKING", "error", err, "group", p.groupName, "batch_size", len(batch), "first_ids", firstN(msgIDs, 5))
		}
		return err
	}

	if p.logger != nil {
		workerIdx := 0
		if idx := strings.LastIndex(workerID, "-w"); idx != -1 {
			if val, err := strconv.Atoi(workerID[idx+2:]); err == nil {
				workerIdx = val
			}
		}
		for _, e := range batch {
			rec := adLogRecordPool.Get().(*pb.AdLogRecord)
			rec.TimestampUnix = e.CreatedAt.Unix()
			if cap(rec.CampaignId) < 16 {
				rec.CampaignId = make([]byte, 16)
			} else {
				rec.CampaignId = rec.CampaignId[:16]
			}
			copy(rec.CampaignId, e.CampaignID[:])
			rec.ClickId = UnsafeBytes(e.ClickID)
			rec.EventType = UnsafeBytes(e.Type)
			rec.Priority = 0

			size := rec.SizeVT()
			bufPtr := logBufPool.Get().(*[]byte)
			buf := *bufPtr
			if cap(buf) < size {
				buf = make([]byte, size)
			} else {
				buf = buf[:size]
			}

			n, err := rec.MarshalToSizedBufferVT(buf)
			if err == nil {
				p.logger.WriteToShard(workerIdx, 0, buf[:n])
			}
			*bufPtr = buf
			logBufPool.Put(bufPtr)

			campIDSaved := rec.CampaignId
			rec.Reset()
			if cap(campIDSaved) >= 16 {
				rec.CampaignId = campIDSaved[:0]
			}
			adLogRecordPool.Put(rec)
		}
	}

	ackCtx, cancel := context.WithTimeout(ctx, p.writeTimeout)
	defer cancel()
	if err := p.rdb.XAck(ackCtx, p.streamName, p.groupName, msgIDs...).Err(); err != nil {
		if !errors.Is(err, context.Canceled) {
			slog.Error("xack failed after successful store", "error", err, "group", p.groupName, "batch_size", len(batch), "first_ids", firstN(msgIDs, 5))
		}
		return err
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

			if err := p.flushBatch(ctx, batch, msgIDs, consumerID); err != nil {
				if !errors.Is(err, context.Canceled) {
					p.recordFailure(consumerID)
					slog.Error("recovery flush failed, moving to DLQ", "error", err, "group", p.groupName)
					_ = p.moveToDLQ(ctx, batch, msgIDs, consumerID, 1, fmt.Errorf("recovery flush failed: %w", err))
					_ = p.rdb.HDel(ctx, "ad:events:retries", msgIDs...).Err()
				}
				for _, e := range batch {
					domain.EventPool.Put(e)
				}
				return
			}
			p.recordSuccess(consumerID)
			_ = p.rdb.HDel(ctx, "ad:events:retries", msgIDs...).Err()
			for _, e := range batch {
				domain.EventPool.Put(e)
			}
		}
	}
}

func (p *StreamConsumer) drainNewMessages(ctx context.Context, consumerID string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			streams, err := p.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    p.groupName,
				Consumer: consumerID,
				Streams:  []string{p.streamName, ">"},
				Count:    int64(p.batchSize),
				Block:    50 * time.Millisecond,
			}).Result()

			if err != nil {
				if err == redis.Nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, redis.ErrClosed) || strings.Contains(err.Error(), "client is closed") {
					return
				}
				slog.Error("drain: failed to read from stream", "error", err, "group", p.groupName, "worker", consumerID)
				return
			}

			if len(streams) == 0 || len(streams[0].Messages) == 0 {
				return
			}

			batch := make([]*domain.Event, 0, len(streams[0].Messages))
			msgIDs := make([]string, 0, len(streams[0].Messages))

			for _, msg := range streams[0].Messages {
				batch = append(batch, p.parseMessage(msg.ID, msg.Values))
				msgIDs = append(msgIDs, msg.ID)
			}

			if err := p.flushBatch(ctx, batch, msgIDs, consumerID); err != nil {
				if !errors.Is(err, context.Canceled) {
					slog.Error("drain: failed to flush batch", "error", err, "group", p.groupName, "worker", consumerID)
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
	defer func() {
		if r := recover(); r != nil {
			slog.Error("janitor panic recovered - exiting process", "error", r)
			os.Exit(1)
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
			pipe := p.rdb.Pipeline()
			incrCmds := make([]*redis.IntCmd, len(entries))
			for i, msg := range entries {
				incrCmds[i] = pipe.HIncrBy(ctx, "ad:events:retries", msg.ID, 1)
			}
			_, _ = pipe.Exec(ctx)

			batch := make([]*domain.Event, 0, len(entries))
			msgIDs := make([]string, 0, len(entries))
			var dlqBatch []*domain.Event
			var dlqMsgIDs []string
			var delMsgIDs []string

			for i, msg := range entries {
				evt := p.parseMessage(msg.ID, msg.Values)
				count, _ := incrCmds[i].Result()
				if count > int64(p.maxRetries) {
					dlqBatch = append(dlqBatch, evt)
					dlqMsgIDs = append(dlqMsgIDs, msg.ID)
					delMsgIDs = append(delMsgIDs, msg.ID)
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
				if len(delMsgIDs) > 0 {
					_ = p.rdb.HDel(ctx, "ad:events:retries", delMsgIDs...).Err()
				}
			}

			if len(batch) > 0 {
				if err := p.flushBatch(ctx, batch, msgIDs, "janitor"); err != nil {
					p.recordFailure("janitor")
					if !errors.Is(err, context.Canceled) {
						slog.Error("janitor flush failed", "error", err, "group", p.groupName)
					}
				} else {
					p.recordSuccess("janitor")
					_ = p.rdb.HDel(ctx, "ad:events:retries", msgIDs...).Err()
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
			slog.Error("dlq monitor panic recovered - exiting process", "error", r)
			os.Exit(1)
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
