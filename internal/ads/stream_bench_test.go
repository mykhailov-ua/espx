package ads

import (
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"espx/internal/ads/pb"
	"espx/internal/domain"
	"github.com/google/uuid"
)

func BenchmarkStreamWriteFlat(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		valuesPtr := producerValuesPool.Get().(*[]any)
		values := *valuesPtr
		values[1] = "dummy-payload"
		producerValuesPool.Put(valuesPtr)
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
		pbEvt := streamEventPool.Get().(*pb.AdStreamEvent)
		pbEvt.ClickId = append(pbEvt.ClickId[:0], evt.ClickID...)
		pbEvt.CampaignId = append(pbEvt.CampaignId[:0], evt.CampaignID[:]...)
		pbEvt.EventType = append(pbEvt.EventType[:0], evt.Type...)
		pbEvt.Payload = append(pbEvt.Payload[:0], evt.Payload...)
		pbEvt.Ip = append(pbEvt.Ip[:0], evt.IP...)
		pbEvt.Ua = append(pbEvt.Ua[:0], evt.UA...)
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
			b.Fatalf("failed to marshal: %v", err)
		}
		data := buf[:n]

		valuesPtr := producerValuesPool.Get().(*[]any)
		values := *valuesPtr
		wrap := byteSliceValuePool.Get().(*ByteSliceValue)
		wrap.b = data
		values[1] = wrap

		DeepResetAdStreamEvent(pbEvt)
		streamEventPool.Put(pbEvt)
		*bufPtr = buf
		byteBufPool.Put(bufPtr)
		byteSliceValuePool.Put(wrap)
		producerValuesPool.Put(valuesPtr)
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
		ClickId:       []byte(evtSetup.ClickID),
		CampaignId:    evtSetup.CampaignID[:],
		EventType:     []byte(evtSetup.Type),
		Payload:       evtSetup.Payload,
		Ip:            []byte(evtSetup.IP),
		Ua:            []byte(evtSetup.UA),
		CreatedAtUnix: evtSetup.CreatedAt.Unix(),
	}
	data, _ := pbEvtSetup.MarshalVT()
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
			DeepResetAdStreamEvent(pbEvt)

			buf := unsafe.Slice(unsafe.StringData(rawBytesStr), len(rawBytesStr))
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
			}
			DeepResetAdStreamEvent(pbEvt)
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

	flatSize := len("click_id") + len(evt.ClickID) +
		len("campaign_id") + len(evt.CampaignID.String()) +
		len("type") + len(evt.Type) +
		len("payload") + len(evt.Payload) +
		len("ip") + len(evt.IP) +
		len("ua") + len(evt.UA)

	pbEvt := &pb.AdStreamEvent{
		ClickId:       []byte(evt.ClickID),
		CampaignId:    evt.CampaignID[:],
		EventType:     []byte(evt.Type),
		Payload:       evt.Payload,
		Ip:            []byte(evt.IP),
		Ua:            []byte(evt.UA),
		CreatedAtUnix: evt.CreatedAt.Unix(),
	}
	protoBytes, _ := pbEvt.MarshalVT()
	protoSize := len(protoBytes)

	t.Logf("PAYLOAD SIZE COMPARISON")
	t.Logf("Flat map raw size: %d bytes", flatSize)
	t.Logf("Protobuf binary size: %d bytes", protoSize)
	t.Logf("Size reduction: %.1f%%", float64(flatSize-protoSize)/float64(flatSize)*100.0)
}

func BenchmarkDLQWriteFlat(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		valuesPtr := dlqValuesPool.Get().(*[]any)
		values := *valuesPtr
		values[1] = "dummy-payload"
		dlqValuesPool.Put(valuesPtr)
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
			DeepResetAdStreamEvent(pbDLQ.OriginalEvent)
		}
		pbDLQ.Error = append(pbDLQ.Error[:0], errVal...)
		pbDLQ.OriginalId = append(pbDLQ.OriginalId[:0], msgID...)
		pbDLQ.FailedAtUnix = time.Now().Unix()
		pbDLQ.WorkerId = append(pbDLQ.WorkerId[:0], workerID...)
		pbDLQ.RetryCount = 3

		pbDLQ.OriginalEvent.ClickId = append(pbDLQ.OriginalEvent.ClickId[:0], evt.ClickID...)
		pbDLQ.OriginalEvent.CampaignId = append(pbDLQ.OriginalEvent.CampaignId[:0], evt.CampaignID[:]...)
		pbDLQ.OriginalEvent.EventType = append(pbDLQ.OriginalEvent.EventType[:0], evt.Type...)
		pbDLQ.OriginalEvent.Payload = append(pbDLQ.OriginalEvent.Payload[:0], evt.Payload...)
		pbDLQ.OriginalEvent.Ip = append(pbDLQ.OriginalEvent.Ip[:0], evt.IP...)
		pbDLQ.OriginalEvent.Ua = append(pbDLQ.OriginalEvent.Ua[:0], evt.UA...)
		pbDLQ.OriginalEvent.CreatedAtUnix = evt.CreatedAt.Unix()

		size := pbDLQ.SizeVT()
		bufPtr := byteBufPool.Get().(*[]byte)
		buf := *bufPtr
		if cap(buf) < size {
			buf = make([]byte, size)
		} else {
			buf = buf[:size]
		}

		n, err := pbDLQ.MarshalToSizedBufferVT(buf)
		if err != nil {
			b.Fatalf("failed to marshal: %v", err)
		}
		data := buf[:n]

		valuesPtr := dlqValuesPool.Get().(*[]any)
		values := *valuesPtr
		wrap := byteSliceValuePool.Get().(*ByteSliceValue)
		wrap.b = data
		values[1] = wrap

		DeepResetAdDLQEvent(pbDLQ)
		dlqEventPool.Put(pbDLQ)
		*bufPtr = buf
		byteBufPool.Put(bufPtr)
		byteSliceValuePool.Put(wrap)
		dlqValuesPool.Put(valuesPtr)
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
			ClickId:       []byte(evt.ClickID),
			CampaignId:    evt.CampaignID[:],
			EventType:     []byte(evt.Type),
			Payload:       evt.Payload,
			Ip:            []byte(evt.IP),
			Ua:            []byte(evt.UA),
			CreatedAtUnix: evt.CreatedAt.Unix(),
		},
		Error:        []byte(errVal),
		OriginalId:   []byte(msgID),
		FailedAtUnix: evt.CreatedAt.Unix(),
		WorkerId:     []byte(workerID),
		RetryCount:   3,
	}
	protoBytes, _ := pbDLQ.MarshalVT()
	protoSize := len(protoBytes)

	t.Logf("DLQ PAYLOAD SIZE COMPARISON")
	t.Logf("Flat map DLQ raw size: %d bytes", flatSize)
	t.Logf("Protobuf DLQ binary size: %d bytes", protoSize)
	t.Logf("Size reduction: %.1f%%", float64(flatSize-protoSize)/float64(flatSize)*100.0)
}
