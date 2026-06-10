package ads

import (
	"context"
	"sync"
	"time"

	"espx/internal/ads/pb"
	"espx/internal/domain"
	"espx/internal/metrics"
	"github.com/google/uuid"
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

// maxStreamLen should be >= maxWorkers * batchSize * expected_flush_lag to avoid data loss.
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
