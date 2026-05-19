package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	"github.com/redis/go-redis/v9"
)

func main() {
	var (
		action   = flag.String("action", "archive", "Action to perform: archive or requeue")
		stream   = flag.String("stream", "ad:events:dlq", "DLQ stream name")
		dest     = flag.String("dest", "dlq_archive.jsonl", "Destination file for archive or stream name for requeue")
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

	encoder := json.NewEncoder(file)
	startID := "0-0"
	var totalProcessed int64

	log.Printf("Starting archive of stream %s to %s", stream, destFile)

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
			if err := encoder.Encode(msg.Values); err != nil {
				return fmt.Errorf("failed to encode message %s: %w", msg.ID, err)
			}
			msgIDs = append(msgIDs, msg.ID)
			startID = msg.ID
		}

		pipe.XDel(ctx, stream, msgIDs...)
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("failed to delete archived messages: %w", err)
		}

		totalProcessed += int64(len(msgIDs))
		log.Printf("Archived %d messages (Total: %d)", len(msgIDs), totalProcessed)
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
			// Extract original values, removing DLQ metadata
			values := make(map[string]interface{})
			for k, v := range msg.Values {
				if k != "error" && k != "original_id" && k != "failed_at" && k != "service" && k != "worker_id" && k != "retry_count" {
					values[k] = v
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
