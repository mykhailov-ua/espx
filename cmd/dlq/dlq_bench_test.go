package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/pb"
	"google.golang.org/protobuf/proto"
)

func TestDLQBackupMetrics(t *testing.T) {
	const count = 10000
	t.Logf("Generating %d mock DLQ events for metrics analysis...", count)

	events := make([]*pb.AdDLQEvent, count)
	for i := 0; i < count; i++ {
		cid := uuid.New()
		events[i] = &pb.AdDLQEvent{
			OriginalEvent: &pb.AdStreamEvent{
				ClickId:       ads.UnsafeBytes(fmt.Sprintf("click_%d_%s", i, uuid.New().String())),
				CampaignId:    cid[:],
				EventType:     []byte("click"),
				Payload:       []byte(`{"geo":"US","device":"mobile","ad_position":"top","network":"search"}`),
				Ip:            []byte("192.168.1.100"),
				Ua:            []byte("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"),
				CreatedAtUnix: time.Now().Unix() - int64(i),
			},
			Error:        []byte("database connection timeout during click processing"),
			OriginalId:   ads.UnsafeBytes(fmt.Sprintf("msg_%d", i)),
			FailedAtUnix: time.Now().Unix(),
			WorkerId:     []byte("worker_node_03_tuf"),
			RetryCount:   int32(i % 5),
		}
	}

	tempDir, err := os.MkdirTemp("", "dlq-metrics-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	jsonlPath := filepath.Join(tempDir, "backup.jsonl")
	binPath := filepath.Join(tempDir, "backup.bin")

	t.Log("MEASURING WRITE SPEED & FILE SIZE")

	startJSONWrite := time.Now()
	jsonFile, err := os.Create(jsonlPath)
	if err != nil {
		t.Fatalf("failed to create jsonl file: %v", err)
	}
	encoder := json.NewEncoder(jsonFile)
	for _, ev := range events {
		campUUID, _ := uuid.FromBytes(ev.OriginalEvent.CampaignId)
		record := map[string]interface{}{
			"id":             ads.UnsafeString(ev.OriginalId),
			"format":         "protobuf_dlq",
			"error":          ads.UnsafeString(ev.Error),
			"original_id":    ads.UnsafeString(ev.OriginalId),
			"failed_at_unix": ev.FailedAtUnix,
			"worker_id":      ads.UnsafeString(ev.WorkerId),
			"retry_count":    ev.RetryCount,
			"original_event": map[string]interface{}{
				"click_id":        ads.UnsafeString(ev.OriginalEvent.ClickId),
				"campaign_id":     campUUID.String(),
				"event_type":      ads.UnsafeString(ev.OriginalEvent.EventType),
				"payload":         ads.UnsafeString(ev.OriginalEvent.Payload),
				"ip":              ads.UnsafeString(ev.OriginalEvent.Ip),
				"ua":              ads.UnsafeString(ev.OriginalEvent.Ua),
				"created_at_unix": ev.OriginalEvent.CreatedAtUnix,
			},
		}
		if err := encoder.Encode(record); err != nil {
			t.Fatalf("json encode fail: %v", err)
		}
	}
	jsonFile.Close()
	durJSONWrite := time.Since(startJSONWrite)

	startBinWrite := time.Now()
	binFile, err := os.Create(binPath)
	if err != nil {
		t.Fatalf("failed to create binary file: %v", err)
	}
	for _, ev := range events {
		data, err := proto.Marshal(ev)
		if err != nil {
			t.Fatalf("protobuf marshal fail: %v", err)
		}
		var lengthBuf [4]byte
		binary.BigEndian.PutUint32(lengthBuf[:], uint32(len(data)))
		if _, err := binFile.Write(lengthBuf[:]); err != nil {
			t.Fatalf("write fail: %v", err)
		}
		if _, err := binFile.Write(data); err != nil {
			t.Fatalf("write fail: %v", err)
		}
	}
	binFile.Close()
	durBinWrite := time.Since(startBinWrite)

	jsonInfo, _ := os.Stat(jsonlPath)
	binInfo, _ := os.Stat(binPath)

	t.Logf("[JSONL Backup]  Time: %v, Size: %.2f KB", durJSONWrite, float64(jsonInfo.Size())/1024)
	t.Logf("[Binary Backup] Time: %v, Size: %.2f KB", durBinWrite, float64(binInfo.Size())/1024)
	t.Logf("-> Size Reduction: %.1f%%", float64(jsonInfo.Size()-binInfo.Size())/float64(jsonInfo.Size())*100)
	t.Logf("-> Write Speedup: %.1fx", float64(durJSONWrite.Nanoseconds())/float64(durBinWrite.Nanoseconds()))

	t.Log("\nMEASURING READ & DECODE (DESERIALIZATION) SPEED")

	startJSONRead := time.Now()
	jsonReadVal, err := os.Open(jsonlPath)
	if err != nil {
		t.Fatalf("failed to open jsonl file: %v", err)
	}
	decoder := json.NewDecoder(jsonReadVal)
	var decodedJSONCount int
	for {
		var record map[string]interface{}
		if err := decoder.Decode(&record); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("json decode fail: %v", err)
		}
		decodedJSONCount++
	}
	jsonReadVal.Close()
	durJSONRead := time.Since(startJSONRead)

	startBinRead := time.Now()
	binReadVal, err := os.Open(binPath)
	if err != nil {
		t.Fatalf("failed to open binary file: %v", err)
	}
	var decodedBinCount int
	var lenBuf [4]byte
	for {
		_, err := binReadVal.Read(lenBuf[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("length read fail: %v", err)
		}
		length := binary.BigEndian.Uint32(lenBuf[:])
		data := make([]byte, length)
		if _, err := io.ReadFull(binReadVal, data); err != nil {
			t.Fatalf("payload read fail: %v", err)
		}
		pbDLQ := &pb.AdDLQEvent{}
		if err := proto.Unmarshal(data, pbDLQ); err != nil {
			t.Fatalf("protobuf unmarshal fail: %v", err)
		}
		decodedBinCount++
	}
	binReadVal.Close()
	durBinRead := time.Since(startBinRead)

	t.Logf("[JSONL Read]   Decoded: %d events in %v", decodedJSONCount, durJSONRead)
	t.Logf("[Binary Read]  Decoded: %d events in %v", decodedBinCount, durBinRead)
	t.Logf("-> Read/Decode Speedup: %.1fx", float64(durJSONRead.Nanoseconds())/float64(durBinRead.Nanoseconds()))
}
