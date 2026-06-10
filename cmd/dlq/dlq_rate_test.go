package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/pb"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
	"google.golang.org/protobuf/proto"
)

func setupRateTestRedis(t *testing.T) (*redis.Client, func()) {
	ctx := context.Background()

	redisContainer, err := rediscontainer.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("failed to start redis container: %s", err)
	}

	endpoint, err := redisContainer.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("failed to get redis endpoint: %s", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: endpoint,
	})

	return rdb, func() {
		_ = rdb.Close()
		_ = redisContainer.Terminate(ctx)
	}
}

func TestRequeueDLQ_RateLimiting(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	rdb, cleanup := setupRateTestRedis(t)
	defer cleanup()

	ctx := context.Background()
	dlqStream := "test:dlq"
	targetStream := "test:target"

	const eventCount = 40
	for i := 0; i < eventCount; i++ {
		cid := uuid.New()
		evt := &pb.AdDLQEvent{
			OriginalEvent: &pb.AdStreamEvent{
				ClickId:    []byte(fmt.Sprintf("click_%d", i)),
				CampaignId: cid[:],
				EventType:  []byte("click"),
			},
			Error:      []byte("simulated error"),
			OriginalId: []byte(fmt.Sprintf("msg_%d", i)),
		}
		data, err := proto.Marshal(evt)
		if err != nil {
			t.Fatal(err)
		}

		err = rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: dlqStream,
			Values: map[string]interface{}{
				"d": ads.UnsafeString(data),
			},
		}).Err()
		if err != nil {
			t.Fatal(err)
		}
	}

	start := time.Now()
	err := requeueDLQ(ctx, rdb, dlqStream, targetStream, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	elapsedUnlimited := time.Since(start)
	t.Logf("Unlimited requeued %d events in %v", eventCount, elapsedUnlimited)

	lenTarget, err := rdb.XLen(ctx, targetStream).Result()
	if err != nil {
		t.Fatal(err)
	}
	if lenTarget != eventCount {
		t.Errorf("expected %d target events, got %d", eventCount, lenTarget)
	}

	rdb.Del(ctx, dlqStream, targetStream)

	for i := 0; i < eventCount; i++ {
		cid := uuid.New()
		evt := &pb.AdDLQEvent{
			OriginalEvent: &pb.AdStreamEvent{
				ClickId:    []byte(fmt.Sprintf("click_%d", i)),
				CampaignId: cid[:],
				EventType:  []byte("click"),
			},
			Error:      []byte("simulated error"),
			OriginalId: []byte(fmt.Sprintf("msg_%d", i)),
		}
		data, err := proto.Marshal(evt)
		if err != nil {
			t.Fatal(err)
		}
		rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: dlqStream,
			Values: map[string]interface{}{
				"d": ads.UnsafeString(data),
			},
		})
	}

	start = time.Now()
	err = requeueDLQ(ctx, rdb, dlqStream, targetStream, 100, 20)
	if err != nil {
		t.Fatal(err)
	}
	elapsedThrottled := time.Since(start)
	t.Logf("Throttled requeued %d events in %v (target: 20 events/sec)", eventCount, elapsedThrottled)

	if elapsedThrottled < 900*time.Millisecond {
		t.Errorf("expected throttled requeue to take at least 900ms, took %v", elapsedThrottled)
	}

	lenTarget, err = rdb.XLen(ctx, targetStream).Result()
	if err != nil {
		t.Fatal(err)
	}
	if lenTarget != eventCount {
		t.Errorf("expected %d target events, got %d", eventCount, lenTarget)
	}
}

func TestRestoreDLQ_RateLimiting(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	rdb, cleanup := setupRateTestRedis(t)
	defer cleanup()

	ctx := context.Background()
	targetStream := "test:target-restore"

	tempDir, err := os.MkdirTemp("", "dlq-restore-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	archivePath := filepath.Join(tempDir, "archive.bin")

	const eventCount = 40
	file, err := os.Create(archivePath)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < eventCount; i++ {
		cid := uuid.New()
		evt := &pb.AdDLQEvent{
			OriginalEvent: &pb.AdStreamEvent{
				ClickId:    []byte(fmt.Sprintf("click_%d", i)),
				CampaignId: cid[:],
				EventType:  []byte("click"),
			},
			Error:      []byte("simulated error"),
			OriginalId: []byte(fmt.Sprintf("msg_%d", i)),
		}
		data, err := proto.Marshal(evt)
		if err != nil {
			t.Fatal(err)
		}
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
		file.Write(lenBuf[:])
		file.Write(data)
	}
	file.Close()

	start := time.Now()
	err = restoreDLQ(ctx, rdb, archivePath, targetStream, 10, 20)
	if err != nil {
		t.Fatal(err)
	}
	elapsedThrottled := time.Since(start)
	t.Logf("Throttled restored %d events in %v (target: 20 events/sec)", eventCount, elapsedThrottled)

	if elapsedThrottled < 900*time.Millisecond {
		t.Errorf("expected throttled restore to take at least 900ms, took %v", elapsedThrottled)
	}

	lenTarget, err := rdb.XLen(ctx, targetStream).Result()
	if err != nil {
		t.Fatal(err)
	}
	if lenTarget != eventCount {
		t.Errorf("expected %d target events, got %d", eventCount, lenTarget)
	}
}
