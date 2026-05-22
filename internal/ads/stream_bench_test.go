package ads

import (
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/pb"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"google.golang.org/protobuf/proto"
)

func BenchmarkStreamWriteFlat(b *testing.B) {
	evt := &domain.Event{
		ClickID:    "c_12345_67890_abcdef",
		CampaignID: uuid.New(),
		UserID:     "u_12345",
		Type:       "impression",
		Payload:    []byte(`{"geo":"US","device":"mobile"}`),
		IP:         "192.168.1.1",
		UA:         "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
		CreatedAt:  time.Now(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Mimics old StreamProducer flat map population
		m := map[string]interface{}{
			"click_id":    evt.ClickID,
			"campaign_id": evt.CampaignID.String(),
			"type":        evt.Type,
			"payload":     evt.Payload,
			"ip":          evt.IP,
			"ua":          evt.UA,
		}
		_ = m
	}
}

func BenchmarkStreamWriteProto(b *testing.B) {
	evt := &domain.Event{
		ClickID:    "c_12345_67890_abcdef",
		CampaignID: uuid.New(),
		UserID:     "u_12345",
		Type:       "impression",
		Payload:    []byte(`{"geo":"US","device":"mobile"}`),
		IP:         "192.168.1.1",
		UA:         "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
		CreatedAt:  time.Now(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Mimics new StreamProducer protobuf serialization
		pbEvt := streamEventPool.Get().(*pb.AdStreamEvent)
		pbEvt.Reset()

		pbEvt.ClickId = evt.ClickID
		pbEvt.CampaignId = evt.CampaignID[:]
		pbEvt.EventType = evt.Type
		pbEvt.Payload = evt.Payload
		pbEvt.Ip = evt.IP
		pbEvt.Ua = evt.UA
		pbEvt.CreatedAtUnix = evt.CreatedAt.Unix()

		bufPtr := byteBufPool.Get().(*[]byte)
		buf := (*bufPtr)[:0]

		data, err := proto.MarshalOptions{}.MarshalAppend(buf, pbEvt)
		if err != nil {
			b.Fatalf("failed to marshal: %v", err)
		}
		streamEventPool.Put(pbEvt)
		*bufPtr = data
		byteBufPool.Put(bufPtr)
		_ = data
	}
}

func BenchmarkStreamReadFlat(b *testing.B) {
	cid := uuid.New()
	values := map[string]interface{}{
		"click_id":    "c_12345_67890_abcdef",
		"campaign_id": cid.String(),
		"type":        "impression",
		"payload":     `{"geo":"US","device":"mobile"}`,
		"ip":          "192.168.1.1",
		"ua":          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		evt := domain.EventPool.Get().(*domain.Event)
		evt.Reset()

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

		if evt.CreatedAt.IsZero() {
			id := "1716186000000-0"
			if idx := strings.IndexByte(id, '-'); idx > 0 {
				ms, err := strconv.ParseInt(id[:idx], 10, 64)
				if err == nil {
					evt.CreatedAt = time.Unix(0, ms*int64(time.Millisecond))
				}
			}
		}

		domain.EventPool.Put(evt)
	}
}

func BenchmarkStreamReadProto(b *testing.B) {
	evtSetup := &domain.Event{
		ClickID:    "c_12345_67890_abcdef",
		CampaignID: uuid.New(),
		UserID:     "u_12345",
		Type:       "impression",
		Payload:    []byte(`{"geo":"US","device":"mobile"}`),
		IP:         "192.168.1.1",
		UA:         "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
		CreatedAt:  time.Now(),
	}

	pbEvtSetup := &pb.AdStreamEvent{
		ClickId:       evtSetup.ClickID,
		CampaignId:    evtSetup.CampaignID[:],
		EventType:     evtSetup.Type,
		Payload:       evtSetup.Payload,
		Ip:            evtSetup.IP,
		Ua:            evtSetup.UA,
		CreatedAtUnix: evtSetup.CreatedAt.Unix(),
	}
	data, _ := proto.Marshal(pbEvtSetup)
	rawBytesStr := string(data)

	values := map[string]interface{}{
		"d": rawBytesStr,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
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
			}
			streamEventPool.Put(pbEvt)
		}

		domain.EventPool.Put(evt)
	}
}

func TestStreamPayloadSizeComparison(t *testing.T) {
	evt := &domain.Event{
		ClickID:    "c_12345_67890_abcdef",
		CampaignID: uuid.New(),
		UserID:     "u_12345",
		Type:       "impression",
		Payload:    []byte(`{"geo":"US","device":"mobile"}`),
		IP:         "192.168.1.1",
		UA:         "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
		CreatedAt:  time.Now(),
	}

	// Flat representation size estimation
	flatSize := len("click_id") + len(evt.ClickID) +
		len("campaign_id") + len(evt.CampaignID.String()) +
		len("type") + len(evt.Type) +
		len("payload") + len(evt.Payload) +
		len("ip") + len(evt.IP) +
		len("ua") + len(evt.UA)

	// Proto representation size
	pbEvt := &pb.AdStreamEvent{
		ClickId:       evt.ClickID,
		CampaignId:    evt.CampaignID[:],
		EventType:     evt.Type,
		Payload:       evt.Payload,
		Ip:            evt.IP,
		Ua:            evt.UA,
		CreatedAtUnix: evt.CreatedAt.Unix(),
	}
	protoBytes, _ := proto.Marshal(pbEvt)
	protoSize := len(protoBytes)

	t.Logf("--- PAYLOAD SIZE COMPARISON ---")
	t.Logf("Flat map raw size: %d bytes", flatSize)
	t.Logf("Protobuf binary size: %d bytes", protoSize)
	t.Logf("Size reduction: %.1f%%", float64(flatSize-protoSize)/float64(flatSize)*100.0)
}

func BenchmarkDLQWriteFlat(b *testing.B) {
	evt := &domain.Event{
		ClickID:    "c_12345_67890_abcdef",
		CampaignID: uuid.New(),
		UserID:     "u_12345",
		Type:       "impression",
		Payload:    []byte(`{"geo":"US","device":"mobile"}`),
		IP:         "192.168.1.1",
		UA:         "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
		CreatedAt:  time.Now(),
	}
	errVal := "simulated poison pill error message"
	msgID := "1716186000000-0"
	workerID := "worker-asus-tuf-8f5b27"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		m := map[string]interface{}{
			"click_id":    evt.ClickID,
			"campaign_id": evt.CampaignID.String(),
			"type":        evt.Type,
			"payload":     evt.Payload,
			"ip":          evt.IP,
			"ua":          evt.UA,
			"error":       errVal,
			"original_id": msgID,
			"failed_at":   time.Now().Format(time.RFC3339),
			"service":     "ad-event-processor",
			"worker_id":   workerID,
			"retry_count": 3,
		}
		_ = m
	}
}

func BenchmarkDLQWriteProto(b *testing.B) {
	evt := &domain.Event{
		ClickID:    "c_12345_67890_abcdef",
		CampaignID: uuid.New(),
		UserID:     "u_12345",
		Type:       "impression",
		Payload:    []byte(`{"geo":"US","device":"mobile"}`),
		IP:         "192.168.1.1",
		UA:         "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
		CreatedAt:  time.Now(),
	}
	errVal := "simulated poison pill error message"
	msgID := "1716186000000-0"
	workerID := "worker-asus-tuf-8f5b27"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		pbDLQ := dlqEventPool.Get().(*pb.AdDLQEvent)
		if pbDLQ.OriginalEvent == nil {
			pbDLQ.OriginalEvent = new(pb.AdStreamEvent)
		} else {
			pbDLQ.OriginalEvent.Reset()
		}
		pbDLQ.Error = errVal
		pbDLQ.OriginalId = msgID
		pbDLQ.FailedAtUnix = time.Now().Unix()
		pbDLQ.WorkerId = workerID
		pbDLQ.RetryCount = 3

		pbDLQ.OriginalEvent.ClickId = evt.ClickID
		pbDLQ.OriginalEvent.CampaignId = evt.CampaignID[:]
		pbDLQ.OriginalEvent.EventType = evt.Type
		pbDLQ.OriginalEvent.Payload = evt.Payload
		pbDLQ.OriginalEvent.Ip = evt.IP
		pbDLQ.OriginalEvent.Ua = evt.UA
		pbDLQ.OriginalEvent.CreatedAtUnix = evt.CreatedAt.Unix()

		bufPtr := byteBufPool.Get().(*[]byte)
		buf := (*bufPtr)[:0]

		data, err := proto.MarshalOptions{}.MarshalAppend(buf, pbDLQ)
		if err != nil {
			b.Fatalf("failed to marshal: %v", err)
		}
		dlqEventPool.Put(pbDLQ)
		*bufPtr = data
		byteBufPool.Put(bufPtr)
		_ = data
	}
}

func TestDLQPayloadSizeComparison(t *testing.T) {
	evt := &domain.Event{
		ClickID:    "c_12345_67890_abcdef",
		CampaignID: uuid.New(),
		UserID:     "u_12345",
		Type:       "impression",
		Payload:    []byte(`{"geo":"US","device":"mobile"}`),
		IP:         "192.168.1.1",
		UA:         "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
		CreatedAt:  time.Now(),
	}
	errVal := "simulated poison pill error message"
	msgID := "1716186000000-0"
	workerID := "worker-asus-tuf-8f5b27"

	flatSize := len("click_id") + len(evt.ClickID) +
		len("campaign_id") + len(evt.CampaignID.String()) +
		len("type") + len(evt.Type) +
		len("payload") + len(evt.Payload) +
		len("ip") + len(evt.IP) +
		len("ua") + len(evt.UA) +
		len("error") + len(errVal) +
		len("original_id") + len(msgID) +
		len("failed_at") + len(time.RFC3339) +
		len("service") + len("ad-event-processor") +
		len("worker_id") + len(workerID) +
		len("retry_count") + 1

	pbDLQ := &pb.AdDLQEvent{
		OriginalEvent: &pb.AdStreamEvent{
			ClickId:       evt.ClickID,
			CampaignId:    evt.CampaignID[:],
			EventType:     evt.Type,
			Payload:       evt.Payload,
			Ip:            evt.IP,
			Ua:            evt.UA,
			CreatedAtUnix: evt.CreatedAt.Unix(),
		},
		Error:        errVal,
		OriginalId:   msgID,
		FailedAtUnix: evt.CreatedAt.Unix(),
		WorkerId:     workerID,
		RetryCount:   3,
	}
	protoBytes, _ := proto.Marshal(pbDLQ)
	protoSize := len(protoBytes)

	t.Logf("--- DLQ PAYLOAD SIZE COMPARISON ---")
	t.Logf("Flat map DLQ raw size: %d bytes", flatSize)
	t.Logf("Protobuf DLQ binary size: %d bytes", protoSize)
	t.Logf("Size reduction: %.1f%%", float64(flatSize-protoSize)/float64(flatSize)*100.0)
}
