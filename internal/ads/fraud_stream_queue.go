package ads

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"espx/internal/domain"
	"espx/internal/metrics"
	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

// fraudRingCapacity is the power-of-two MPSC ring size for async fraud stream writes.
const (
	fraudRingCapacity = 4096
	fraudRingMask     = fraudRingCapacity - 1
	fraudRingUsable   = fraudRingCapacity - 1
	fraudFlushBatch   = 64

	fraudSlotClickMax   = 128
	fraudSlotUserMax    = 128
	fraudSlotTypeMax    = 32
	fraudSlotIPMax      = 64
	fraudSlotUAMax      = 512
	fraudSlotPayloadMax = 2048
	fraudSlotReasonMax  = 128
)

// fraudStreamSlot stores one fraud event in fixed arrays to avoid heap allocations on enqueue.
type fraudStreamSlot struct {
	ready      atomic.Uint32
	shard      uint8
	_          [3]byte
	campaignID uuid.UUID
	createdAt  int64

	clickLen   uint16
	userLen    uint16
	typeLen    uint16
	ipLen      uint16
	uaLen      uint16
	payloadLen uint16
	reasonLen  uint16

	clickID [fraudSlotClickMax]byte
	userID  [fraudSlotUserMax]byte
	evtType [fraudSlotTypeMax]byte
	ip      [fraudSlotIPMax]byte
	ua      [fraudSlotUAMax]byte
	payload [fraudSlotPayloadMax]byte
	reason  [fraudSlotReasonMax]byte
}

// FraudStreamWriter decouples fraud telemetry from the gnet hot path via a lossy async queue.
type FraudStreamWriter struct {
	_           [64]byte
	writeCursor uint64
	_           [64]byte
	allocCursor uint64
	_           [64]byte
	readCursor  uint64
	_           [64]byte
	slots       [fraudRingCapacity]fraudStreamSlot

	stream string
	maxLen int64
	rdbs   []redis.UniversalClient

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewFraudStreamWriter starts the background drainer when Redis and stream name are configured.
func NewFraudStreamWriter(rdbs []redis.UniversalClient, stream string, maxLen int64) *FraudStreamWriter {
	if len(rdbs) == 0 || stream == "" {
		return nil
	}
	q := &FraudStreamWriter{
		stream: stream,
		maxLen: maxLen,
		rdbs:   rdbs,
		stopCh: make(chan struct{}),
	}
	q.wg.Add(1)
	go q.worker()
	return q
}

// copyFraudField copies a string into a fixed slot buffer and returns the written length.
func copyFraudField(dst []byte, s string) int {
	n := len(s)
	if n > len(dst) {
		n = len(dst)
	}
	if n > 0 {
		copy(dst[:n], s[:n])
	}
	return n
}

// Enqueue copies a fraud event into the ring; false means the ring overflowed and the event is dropped.
func (q *FraudStreamWriter) Enqueue(shard int, evt *domain.Event) bool {
	if q == nil || evt == nil {
		return true
	}
	if shard < 0 || shard >= len(q.rdbs) {
		shard = 0
	}

	for {
		alloc := atomic.LoadUint64(&q.allocCursor)
		read := atomic.LoadUint64(&q.readCursor)
		if alloc-read >= fraudRingUsable {
			for spin := 0; spin < 100; spin++ {
				if spin < 20 {
					runtime.Gosched()
				} else {
					time.Sleep(time.Microsecond)
				}
				read = atomic.LoadUint64(&q.readCursor)
				if alloc-read < fraudRingUsable {
					goto spaceAvailable
				}
			}
			return false
		}
		if alloc-read >= fraudRingUsable-512 {
			runtime.Gosched()
		}
	spaceAvailable:
		if !atomic.CompareAndSwapUint64(&q.allocCursor, alloc, alloc+1) {
			continue
		}

		idx := alloc & fraudRingMask
		slot := &q.slots[idx]
		slot.ready.Store(0)
		slot.shard = uint8(shard)
		slot.campaignID = evt.CampaignID
		slot.createdAt = evt.CreatedAt.UnixNano()
		slot.clickLen = uint16(copyFraudField(slot.clickID[:], evt.ClickID))
		slot.userLen = uint16(copyFraudField(slot.userID[:], evt.UserID))
		slot.typeLen = uint16(copyFraudField(slot.evtType[:], evt.Type))
		slot.ipLen = uint16(copyFraudField(slot.ip[:], evt.IP))
		slot.uaLen = uint16(copyFraudField(slot.ua[:], evt.UA))
		slot.payloadLen = uint16(copyFraudField(slot.payload[:], unsafeString(evt.Payload)))
		slot.reasonLen = uint16(copyFraudField(slot.reason[:], evt.FraudReason))
		slot.ready.Store(1)

		for {
			if atomic.LoadUint64(&q.writeCursor) == alloc {
				atomic.StoreUint64(&q.writeCursor, alloc+1)
				return true
			}
			runtime.Gosched()
		}
	}
}

// Pending returns the number of fraud events not yet flushed to Redis.
func (q *FraudStreamWriter) Pending() uint64 {
	if q == nil {
		return 0
	}
	head := atomic.LoadUint64(&q.writeCursor)
	tail := atomic.LoadUint64(&q.readCursor)
	if head <= tail {
		return 0
	}
	return head - tail
}

// Stop drains pending fraud events and waits for the background worker to exit.
func (q *FraudStreamWriter) Stop() {
	if q == nil {
		return
	}
	select {
	case <-q.stopCh:
		return
	default:
		close(q.stopCh)
	}
	q.wg.Wait()
}

// worker periodically drains the ring and batches XADD calls to Redis.
func (q *FraudStreamWriter) worker() {
	defer q.wg.Done()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-q.stopCh:
			q.drain(true)
			return
		case <-ticker.C:
			q.drain(false)
		}
	}
}

// drain reads ready slots from the ring and flushes them in batches.
func (q *FraudStreamWriter) drain(final bool) {
	valSlicePtr := fraudValuesPool.Get().(*[]any)
	wCamp := bufPool.Get().(*bufWrapper)
	wTime := bufPool.Get().(*bufWrapper)
	defer func() {
		fraudValuesPool.Put(valSlicePtr)
		bufPool.Put(wCamp)
		bufPool.Put(wTime)
	}()

	valSlice := *valSlicePtr
	ctx := context.Background()
	batch := make([]*fraudStreamSlot, 0, fraudFlushBatch)

	for {
		writeCursor := atomic.LoadUint64(&q.writeCursor)
		readCursor := atomic.LoadUint64(&q.readCursor)
		if readCursor >= writeCursor {
			break
		}
		if len(batch) >= fraudFlushBatch {
			q.flushBatch(ctx, batch, valSlice, wCamp, wTime)
			batch = batch[:0]
		}

		idx := readCursor & fraudRingMask
		slot := &q.slots[idx]
		for slot.ready.Load() == 0 {
			runtime.Gosched()
		}
		batch = append(batch, slot)
		atomic.StoreUint64(&q.readCursor, readCursor+1)
	}

	if len(batch) > 0 {
		q.flushBatch(ctx, batch, valSlice, wCamp, wTime)
	}

	if final {
		for atomic.LoadUint64(&q.writeCursor) != atomic.LoadUint64(&q.readCursor) {
			runtime.Gosched()
		}
	}
}

// flushBatch pipelines XADD commands grouped by Redis shard.
func (q *FraudStreamWriter) flushBatch(ctx context.Context, batch []*fraudStreamSlot, valSlice []any, wCamp, wTime *bufWrapper) {
	if len(batch) == 0 {
		return
	}

	type shardBatch struct {
		pipe redis.Pipeliner
		cmds []*redis.StringCmd
	}
	shards := make(map[uint8]*shardBatch)

	for _, slot := range batch {
		shard := slot.shard
		sb, ok := shards[shard]
		if !ok {
			sb = &shardBatch{pipe: q.rdbs[shard].Pipeline()}
			shards[shard] = sb
		}
		fillFraudStreamValuesFromSlot(valSlice, slot, wCamp, wTime)
		cmd := sb.pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: q.stream,
			MaxLen: q.maxLen,
			Approx: true,
			Values: valSlice,
		})
		sb.cmds = append(sb.cmds, cmd)
	}

	for _, sb := range shards {
		_, _ = sb.pipe.Exec(ctx)
		for _, cmd := range sb.cmds {
			if cmd.Err() != nil {
				filterFraudStreamWriteErrors.Inc()
			}
		}
	}
}

// fillFraudStreamValuesFromSlot builds Redis stream field pairs from a ring slot.
func fillFraudStreamValuesFromSlot(valSlice []any, slot *fraudStreamSlot, wCamp, wTime *bufWrapper) {
	wCamp.buf = wCamp.buf[:0]
	wCamp.buf = appendUUID(wCamp.buf, slot.campaignID)
	campIDStr := unsafeString(wCamp.buf)

	wTime.buf = wTime.buf[:0]
	wTime.buf = time.Unix(0, slot.createdAt).AppendFormat(wTime.buf, time.RFC3339Nano)
	timeStr := unsafeString(wTime.buf)

	valSlice[0] = "click_id"
	valSlice[1] = unsafeString(slot.clickID[:slot.clickLen])
	valSlice[2] = "campaign_id"
	valSlice[3] = campIDStr
	valSlice[4] = "user_id"
	valSlice[5] = unsafeString(slot.userID[:slot.userLen])
	valSlice[6] = "type"
	valSlice[7] = unsafeString(slot.evtType[:slot.typeLen])
	valSlice[8] = "ip"
	valSlice[9] = unsafeString(slot.ip[:slot.ipLen])
	valSlice[10] = "ua"
	valSlice[11] = unsafeString(slot.ua[:slot.uaLen])
	valSlice[12] = "payload"
	valSlice[13] = unsafeString(slot.payload[:slot.payloadLen])
	valSlice[14] = "fraud_reason"
	valSlice[15] = unsafeString(slot.reason[:slot.reasonLen])
	valSlice[16] = "created_at"
	valSlice[17] = timeStr
}

// enqueueFraudReject enqueues a rejected fraud event, counting drops when the ring is full.
func enqueueFraudReject(writer *FraudStreamWriter, shard int, evt *domain.Event) {
	if writer == nil {
		return
	}
	if !writer.Enqueue(shard, evt) {
		metrics.FraudStreamDropTotal.Inc()
	}
}
