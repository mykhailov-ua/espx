package ads

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/pb"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/mykhailov-ua/ad-event-processor/internal/metrics"
	redis "github.com/redis/go-redis/v9"
)

var (
	streamEventPool = sync.Pool{
		New: func() any {
			return new(pb.AdStreamEvent)
		},
	}
	byteBufPool = sync.Pool{
		New: func() any {
			b := make([]byte, 0, 512)
			return &b
		},
	}
	producerValuesPool = sync.Pool{
		New: func() any {
			slice := make([]any, 2)
			slice[0] = "d"
			return &slice
		},
	}
)

type StreamProducer struct {
	rdb          redis.UniversalClient
	streamName   string
	maxStreamLen int64
	writeTimeout time.Duration
}

func NewStreamProducer(
	rdb redis.UniversalClient,
	streamName string,
	maxStreamLen int,
	writeTimeout time.Duration,
) *StreamProducer {
	return &StreamProducer{
		rdb:          rdb,
		streamName:   streamName,
		maxStreamLen: int64(maxStreamLen),
		writeTimeout: writeTimeout,
	}
}

// Process serializes and pushes ad stream events into the Redis stream.
//
// Memory Impact:
//   - Bounded heap allocations. Employs recycling pools for Protobuf events (streamEventPool),
//     serialization byte arrays (byteBufPool), and redis parameter containers (producerValuesPool).
//
// Concurrency:
// - Thread-safe. Safe to call concurrently from multiple pipeline processing workers.
//
// Performance Hacks:
//   - vtproto Serialization: Uses vtproto plugins (`MarshalToSizedBufferVT` and `SizeVT`) to compute exact payload size
//     and marshal fields directly into recycled byte arrays without allocating memory.
//   - ByteSliceValue wrapping: Wraps serializations in ByteSliceValue to bypass interface allocations and avoid string copy
//     conversions inside the redis client library.
func (p *StreamProducer) Process(evt *domain.Event) error {
	if evt.ClickID == "" {
		id, err := uuid.NewV7()
		if err != nil {
			return err
		}
		evt.ClickID = id.String()
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.writeTimeout)
	defer cancel()

	pbEvt := streamEventPool.Get().(*pb.AdStreamEvent)
	pbEvt.ClickId = UnsafeBytes(evt.ClickID)
	pbEvt.CampaignId = evt.CampaignID[:]
	pbEvt.EventType = UnsafeBytes(evt.Type)
	pbEvt.Payload = evt.Payload
	pbEvt.Ip = UnsafeBytes(evt.IP)
	pbEvt.Ua = UnsafeBytes(evt.UA)
	pbEvt.CreatedAtUnix = evt.CreatedAt.Unix()

	size := pbEvt.SizeVT()
	bufPtr := byteBufPool.Get().(*[]byte)
	buf := *bufPtr
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}

	n, err := pbEvt.MarshalToSizedBufferVT(buf)
	if err != nil {
		ClearAdStreamEvent(pbEvt)
		streamEventPool.Put(pbEvt)
		*bufPtr = buf
		byteBufPool.Put(bufPtr)
		metrics.EventsDropped.Inc()
		return err
	}
	data := buf[:n]

	valuesPtr := producerValuesPool.Get().(*[]any)
	values := *valuesPtr

	wrap := byteSliceValuePool.Get().(*ByteSliceValue)
	wrap.b = data
	values[1] = wrap

	_, err = p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: p.streamName,
		MaxLen: p.maxStreamLen,
		Approx: true,
		Values: values,
	}).Result()

	ClearAdStreamEvent(pbEvt)
	streamEventPool.Put(pbEvt)
	*bufPtr = buf
	byteBufPool.Put(bufPtr)
	byteSliceValuePool.Put(wrap)
	producerValuesPool.Put(valuesPtr)

	if err != nil {
		metrics.EventsDropped.Inc()
		return err
	}

	metrics.EventsProcessed.Inc()
	return nil
}
