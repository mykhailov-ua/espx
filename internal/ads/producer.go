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
	"google.golang.org/protobuf/proto"
)

var streamEventPool = sync.Pool{
	New: func() any {
		return new(pb.AdStreamEvent)
	},
}

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
	pbEvt.Reset()

	pbEvt.ClickId = evt.ClickID
	pbEvt.CampaignId = evt.CampaignID[:]
	pbEvt.EventType = evt.Type
	pbEvt.Payload = evt.Payload
	pbEvt.Ip = evt.IP
	pbEvt.Ua = evt.UA
	pbEvt.CreatedAtUnix = evt.CreatedAt.Unix()

	data, err := proto.Marshal(pbEvt)
	streamEventPool.Put(pbEvt)
	if err != nil {
		metrics.EventsDropped.Inc()
		return err
	}

	_, err = p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: p.streamName,
		MaxLen: p.maxStreamLen,
		Approx: true,
		Values: []interface{}{"d", data},
	}).Result()

	if err != nil {
		metrics.EventsDropped.Inc()
		return err
	}

	metrics.EventsProcessed.Inc()
	return nil
}
