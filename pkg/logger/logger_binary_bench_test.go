package logger

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
	"unsafe"

	"espx/internal/ads/pb"
	"github.com/google/uuid"
)

type JSONLogRecord struct {
	Level      string `json:"level"`
	Timestamp  string `json:"timestamp"`
	Msg        string `json:"msg"`
	CampaignID string `json:"campaign_id"`
	ClickID    string `json:"click_id"`
	Type       string `json:"type"`
	Priority   int    `json:"priority"`
}

func unsafeBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func BenchmarkSerialization_JSON(b *testing.B) {
	campaignID := uuid.New()
	clickID := "click_1234567890_abc"
	eventType := "click"
	createdAt := time.Now()

	bufPool := sync.Pool{
		New: func() any {
			buf := make([]byte, 0, 512)
			return &buf
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		bufPtr := bufPool.Get().(*[]byte)
		buf := (*bufPtr)[:0]
		buf = append(buf, `{"level":"info","timestamp":"`...)
		buf = createdAt.AppendFormat(buf, time.RFC3339)
		buf = append(buf, `","msg":"event successfully processed","campaign_id":"`...)
		buf = append(buf, campaignID.String()...)
		buf = append(buf, `","click_id":"`...)
		buf = append(buf, clickID...)
		buf = append(buf, `","type":"`...)
		buf = append(buf, eventType...)
		buf = append(buf, `","priority":0}`...)

		_ = buf
		*bufPtr = buf
		bufPool.Put(bufPtr)
	}
}

func BenchmarkSerialization_VtProto(b *testing.B) {
	campaignID := uuid.New()
	clickID := "click_1234567890_abc"
	eventType := "click"
	createdAt := time.Now()

	recPool := sync.Pool{
		New: func() any {
			return &pb.AdLogRecord{}
		},
	}
	bufPool := sync.Pool{
		New: func() any {
			buf := make([]byte, 0, 512)
			return &buf
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rec := recPool.Get().(*pb.AdLogRecord)
		rec.TimestampUnix = createdAt.Unix()
		rec.CampaignId = campaignID[:]
		rec.ClickId = unsafeBytes(clickID)
		rec.EventType = unsafeBytes(eventType)
		rec.Priority = 0

		size := rec.SizeVT()
		bufPtr := bufPool.Get().(*[]byte)
		buf := *bufPtr
		if cap(buf) < size {
			buf = make([]byte, size)
		} else {
			buf = buf[:size]
		}

		_, err := rec.MarshalToSizedBufferVT(buf)
		if err != nil {
			b.Fatal(err)
		}

		*bufPtr = buf
		bufPool.Put(bufPtr)

		rec.Reset()
		recPool.Put(rec)
	}
}

func BenchmarkDeserialization_JSON(b *testing.B) {
	jsonData := []byte(`{"level":"info","timestamp":"2026-06-05T22:36:16Z","msg":"event successfully processed","campaign_id":"12345678-1234-1234-1234-1234567890ab","click_id":"click_1234567890_abc","type":"click","priority":0}`)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var rec JSONLogRecord
		err := json.Unmarshal(jsonData, &rec)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeserialization_VtProto(b *testing.B) {
	campID := uuid.New()
	rec := &pb.AdLogRecord{
		TimestampUnix: time.Now().Unix(),
		CampaignId:    campID[:],
		ClickId:       []byte("click_1234567890_abc"),
		EventType:     []byte("click"),
		Priority:      0,
	}
	protoData, err := rec.MarshalVT()
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var dec pb.AdLogRecord
		err := dec.UnmarshalVT(protoData)
		if err != nil {
			b.Fatal(err)
		}
	}
}
