package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/pb"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

func main() {
	var (
		action   = flag.String("action", "archive", "Action to perform: archive, requeue, restore, or inspect")
		stream   = flag.String("stream", "ad:events:dlq", "DLQ stream name or target stream name")
		dest     = flag.String("dest", "dlq_archive.bin", "Destination file for archive/restore or target stream name for requeue")
		batch    = flag.Int64("batch", 1000, "Batch size for processing")
		redisURL = flag.String("redis", "redis://localhost:6379", "Redis connection string")
	)
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	opt, err := redis.ParseURL(*redisURL)
	if err != nil {
		log.Fatalf("Invalid Redis URL: %v", err)
	}
	rdb := redis.NewClient(opt)
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	switch *action {
	case "archive":
		if err := archiveDLQ(ctx, rdb, *stream, *dest, *batch); err != nil {
			log.Fatalf("Archive failed: %v", err)
		}
	case "requeue":
		if err := requeueDLQ(ctx, rdb, *stream, *dest, *batch); err != nil {
			log.Fatalf("Requeue failed: %v", err)
		}
	case "restore":
		if err := restoreDLQ(ctx, rdb, *dest, *stream, *batch); err != nil {
			log.Fatalf("Restore failed: %v", err)
		}
	case "inspect":
		if err := inspectStream(ctx, rdb, *stream, *batch); err != nil {
			log.Fatalf("Inspect failed: %v", err)
		}
	default:
		log.Fatalf("Unknown action: %s", *action)
	}
}

func archiveDLQ(ctx context.Context, rdb *redis.Client, stream, destFile string, batchSize int64) error {
	file, err := os.OpenFile(destFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open archive file: %w", err)
	}
	defer file.Close()

	startID := "0-0"
	var totalProcessed int64

	log.Printf("Starting binary Protobuf archive of stream %s to %s", stream, destFile)

	for {
		msgs, err := rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{stream, startID},
			Count:   batchSize,
			Block:   time.Millisecond * 10,
		}).Result()

		if err != nil && err != redis.Nil {
			return fmt.Errorf("failed to read from stream: %w", err)
		}

		if len(msgs) == 0 || len(msgs[0].Messages) == 0 {
			break
		}

		pipe := rdb.Pipeline()
		var msgIDs []string

		for _, msg := range msgs[0].Messages {
			pbDLQ := &pb.AdDLQEvent{}

			if rawBytesStr, ok := msg.Values["d"].(string); ok {

				if err := proto.Unmarshal(ads.UnsafeBytes(rawBytesStr), pbDLQ); err != nil {

					pbStream := &pb.AdStreamEvent{}
					if err := proto.Unmarshal(ads.UnsafeBytes(rawBytesStr), pbStream); err == nil {
						pbDLQ.OriginalEvent = pbStream
						pbDLQ.Error = ads.UnsafeBytes("recovered stream event")
						pbDLQ.OriginalId = ads.UnsafeBytes(msg.ID)
						pbDLQ.FailedAtUnix = time.Now().Unix()
					} else {

						pbDLQ.OriginalEvent = &pb.AdStreamEvent{
							Payload: ads.UnsafeBytes(rawBytesStr),
						}
						pbDLQ.Error = ads.UnsafeBytes("unknown binary")
						pbDLQ.OriginalId = ads.UnsafeBytes(msg.ID)
						pbDLQ.FailedAtUnix = time.Now().Unix()
					}
				}
			} else {

				pbStream := &pb.AdStreamEvent{}
				if v, ok := msg.Values["click_id"].(string); ok {
					pbStream.ClickId = ads.UnsafeBytes(v)
				}
				if v, ok := msg.Values["campaign_id"].(string); ok {
					if u, err := uuid.Parse(v); err == nil {
						pbStream.CampaignId = u[:]
					} else {
						pbStream.CampaignId = ads.UnsafeBytes(v)
					}
				}
				if v, ok := msg.Values["type"].(string); ok {
					pbStream.EventType = ads.UnsafeBytes(v)
				}
				if v, ok := msg.Values["payload"].(string); ok {
					pbStream.Payload = ads.UnsafeBytes(v)
				} else if v, ok := msg.Values["payload"].([]byte); ok {
					pbStream.Payload = v
				}
				if v, ok := msg.Values["ip"].(string); ok {
					pbStream.Ip = ads.UnsafeBytes(v)
				}
				if v, ok := msg.Values["ua"].(string); ok {
					pbStream.Ua = ads.UnsafeBytes(v)
				}
				if v, ok := msg.Values["created_at_unix"].(int64); ok {
					pbStream.CreatedAtUnix = v
				}

				pbDLQ.OriginalEvent = pbStream
				if v, ok := msg.Values["error"].(string); ok {
					pbDLQ.Error = ads.UnsafeBytes(v)
				}
				if v, ok := msg.Values["original_id"].(string); ok {
					pbDLQ.OriginalId = ads.UnsafeBytes(v)
				} else {
					pbDLQ.OriginalId = ads.UnsafeBytes(msg.ID)
				}
				if v, ok := msg.Values["failed_at_unix"].(int64); ok {
					pbDLQ.FailedAtUnix = v
				} else {
					pbDLQ.FailedAtUnix = time.Now().Unix()
				}
				if v, ok := msg.Values["worker_id"].(string); ok {
					pbDLQ.WorkerId = ads.UnsafeBytes(v)
				}
				if v, ok := msg.Values["retry_count"].(int64); ok {
					pbDLQ.RetryCount = int32(v)
				}
			}

			data, err := proto.Marshal(pbDLQ)
			if err != nil {
				return fmt.Errorf("failed to marshal message %s: %w", msg.ID, err)
			}

			var lengthBuf [4]byte
			binary.BigEndian.PutUint32(lengthBuf[:], uint32(len(data)))
			if _, err := file.Write(lengthBuf[:]); err != nil {
				return fmt.Errorf("failed to write length prefix for msg %s: %w", msg.ID, err)
			}
			if _, err := file.Write(data); err != nil {
				return fmt.Errorf("failed to write message data for msg %s: %w", msg.ID, err)
			}

			msgIDs = append(msgIDs, msg.ID)
			startID = msg.ID
		}

		pipe.XDel(ctx, stream, msgIDs...)
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("failed to delete archived messages: %w", err)
		}

		totalProcessed += int64(len(msgIDs))
		log.Printf("Archived and deleted %d messages (Total: %d)", len(msgIDs), totalProcessed)
	}

	log.Printf("Archive completed. Total archived: %d", totalProcessed)
	return nil
}

func requeueDLQ(ctx context.Context, rdb *redis.Client, dlqStream, targetStream string, batchSize int64) error {
	startID := "0-0"
	var totalProcessed int64

	log.Printf("Starting requeue from %s to %s", dlqStream, targetStream)

	for {
		msgs, err := rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{dlqStream, startID},
			Count:   batchSize,
			Block:   time.Millisecond * 10,
		}).Result()

		if err != nil && err != redis.Nil {
			return fmt.Errorf("failed to read from stream: %w", err)
		}

		if len(msgs) == 0 || len(msgs[0].Messages) == 0 {
			break
		}

		pipe := rdb.Pipeline()
		var msgIDs []string

		for _, msg := range msgs[0].Messages {
			values := make(map[string]interface{})
			if rawBytesStr, ok := msg.Values["d"].(string); ok {

				pbDLQ := &pb.AdDLQEvent{}
				if err := proto.Unmarshal(ads.UnsafeBytes(rawBytesStr), pbDLQ); err == nil && pbDLQ.OriginalEvent != nil {
					data, err := proto.Marshal(pbDLQ.OriginalEvent)
					if err == nil {
						values["d"] = ads.UnsafeString(data)
					} else {
						log.Printf("Failed to re-marshal original event from DLQ message %s: %v", msg.ID, err)
					}
				} else {
					log.Printf("Failed to unmarshal Protobuf DLQ message %s: %v", msg.ID, err)
				}
			} else {

				for k, v := range msg.Values {
					if k != "error" && k != "original_id" && k != "failed_at" && k != "service" && k != "worker_id" && k != "retry_count" {
						values[k] = v
					}
				}
			}

			pipe.XAdd(ctx, &redis.XAddArgs{
				Stream: targetStream,
				Values: values,
			})
			msgIDs = append(msgIDs, msg.ID)
			startID = msg.ID
		}

		pipe.XDel(ctx, dlqStream, msgIDs...)
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("failed to requeue messages: %w", err)
		}

		totalProcessed += int64(len(msgIDs))
		log.Printf("Requeued %d messages (Total: %d)", len(msgIDs), totalProcessed)
	}

	log.Printf("Requeue completed. Total requeued: %d", totalProcessed)
	return nil
}

func restoreDLQ(ctx context.Context, rdb *redis.Client, srcFile, targetStream string, batchSize int64) error {
	file, err := os.Open(srcFile)
	if err != nil {
		return fmt.Errorf("failed to open archive file: %w", err)
	}
	defer file.Close()

	log.Printf("Starting restore from %s to stream %s", srcFile, targetStream)

	var totalProcessed int64
	var lengthBuf [4]byte
	pipe := rdb.Pipeline()
	batchCount := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, err := file.Read(lengthBuf[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read length prefix: %w", err)
		}

		length := binary.BigEndian.Uint32(lengthBuf[:])
		data := make([]byte, length)
		if _, err := io.ReadFull(file, data); err != nil {
			return fmt.Errorf("failed to read message payload: %w", err)
		}

		pbDLQ := &pb.AdDLQEvent{}
		if err := proto.Unmarshal(data, pbDLQ); err != nil {
			return fmt.Errorf("failed to unmarshal AdDLQEvent: %w", err)
		}

		if pbDLQ.OriginalEvent == nil {
			log.Printf("Warning: AdDLQEvent at offset %d has no original event, skipping", totalProcessed)
			continue
		}

		streamData, err := proto.Marshal(pbDLQ.OriginalEvent)
		if err != nil {
			return fmt.Errorf("failed to marshal original event: %w", err)
		}

		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: targetStream,
			Values: map[string]interface{}{
				"d": ads.UnsafeString(streamData),
			},
		})
		batchCount++
		totalProcessed++

		if int64(batchCount) >= batchSize {
			if _, err := pipe.Exec(ctx); err != nil {
				return fmt.Errorf("failed to restore batch to Redis: %w", err)
			}
			log.Printf("Restored %d messages (Total: %d)", batchCount, totalProcessed)
			pipe = rdb.Pipeline()
			batchCount = 0
		}
	}

	if batchCount > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("failed to restore final batch to Redis: %w", err)
		}
		log.Printf("Restored %d messages (Total: %d)", batchCount, totalProcessed)
	}

	log.Printf("Restore completed. Total restored: %d", totalProcessed)
	return nil
}

func inspectStream(ctx context.Context, rdb *redis.Client, stream string, batchSize int64) error {
	startID := "0-0"
	var totalProcessed int64

	log.Printf("Starting inspection of stream %s", stream)

	for {
		msgs, err := rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{stream, startID},
			Count:   batchSize,
			Block:   time.Millisecond * 10,
		}).Result()

		if err != nil && err != redis.Nil {
			return fmt.Errorf("failed to read from stream: %w", err)
		}

		if len(msgs) == 0 || len(msgs[0].Messages) == 0 {
			break
		}

		for _, msg := range msgs[0].Messages {
			fmt.Printf("\nMessage ID: %s\n", msg.ID)

			if rawBytesStr, ok := msg.Values["d"].(string); ok {

				pbDLQ := &pb.AdDLQEvent{}
				if err := proto.Unmarshal(ads.UnsafeBytes(rawBytesStr), pbDLQ); err == nil && pbDLQ.OriginalEvent != nil {
					fmt.Println("Format: Protobuf (AdDLQEvent)")
					orig := pbDLQ.OriginalEvent
					var campUUIDStr string
					if len(orig.CampaignId) == 16 {
						if u, err := uuid.FromBytes(orig.CampaignId); err == nil {
							campUUIDStr = u.String()
						}
					}
					if campUUIDStr == "" {
						campUUIDStr = ads.UnsafeString(orig.CampaignId)
					}

					m := map[string]interface{}{
						"error":          ads.UnsafeString(pbDLQ.Error),
						"original_id":    ads.UnsafeString(pbDLQ.OriginalId),
						"failed_at_unix": pbDLQ.FailedAtUnix,
						"failed_at":      time.Unix(pbDLQ.FailedAtUnix, 0).Format(time.RFC3339),
						"worker_id":      ads.UnsafeString(pbDLQ.WorkerId),
						"retry_count":    pbDLQ.RetryCount,
						"original_event": map[string]interface{}{
							"click_id":        ads.UnsafeString(orig.ClickId),
							"campaign_id":     campUUIDStr,
							"event_type":      ads.UnsafeString(orig.EventType),
							"payload":         ads.UnsafeString(orig.Payload),
							"ip":              ads.UnsafeString(orig.Ip),
							"ua":              ads.UnsafeString(orig.Ua),
							"created_at_unix": orig.CreatedAtUnix,
							"created_at":      time.Unix(orig.CreatedAtUnix, 0).Format(time.RFC3339),
						},
					}
					prettyJSON, _ := json.MarshalIndent(m, "", "  ")
					fmt.Println(string(prettyJSON))
				} else {

					pbStream := &pb.AdStreamEvent{}
					if err := proto.Unmarshal(ads.UnsafeBytes(rawBytesStr), pbStream); err == nil {
						fmt.Println("Format: Protobuf (AdStreamEvent)")
						var campUUIDStr string
						if len(pbStream.CampaignId) == 16 {
							if u, err := uuid.FromBytes(pbStream.CampaignId); err == nil {
								campUUIDStr = u.String()
							}
						}
						if campUUIDStr == "" {
							campUUIDStr = ads.UnsafeString(pbStream.CampaignId)
						}

						m := map[string]interface{}{
							"click_id":        ads.UnsafeString(pbStream.ClickId),
							"campaign_id":     campUUIDStr,
							"event_type":      ads.UnsafeString(pbStream.EventType),
							"payload":         ads.UnsafeString(pbStream.Payload),
							"ip":              ads.UnsafeString(pbStream.Ip),
							"ua":              ads.UnsafeString(pbStream.Ua),
							"created_at_unix": pbStream.CreatedAtUnix,
							"created_at":      time.Unix(pbStream.CreatedAtUnix, 0).Format(time.RFC3339),
						}
						prettyJSON, _ := json.MarshalIndent(m, "", "  ")
						fmt.Println(string(prettyJSON))
					} else {
						fmt.Println("Format: Unknown Binary Protobuf")
						fmt.Printf("Raw values: %+v\n", msg.Values)
					}
				}
			} else {
				fmt.Println("Format: Legacy Flat Map")
				prettyJSON, _ := json.MarshalIndent(msg.Values, "", "  ")
				fmt.Println(string(prettyJSON))
			}
			startID = msg.ID
			totalProcessed++
		}
	}
	log.Printf("Inspection completed. Total inspected: %d", totalProcessed)
	return nil
}
