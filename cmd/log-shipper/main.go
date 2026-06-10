package main

import (
	"encoding/binary"
	"flag"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"espx/pkg/broker/client"
)

func main() {
	brokerAddr := flag.String("broker", "127.0.0.1:9092", "Broker address")
	redisURL := flag.String("redis-url", "redis://127.0.0.1:6379/0", "Redis URL for leader discovery")
	logFilePath := flag.String("log-file", "/var/log/espx/active.log", "Path to the active log file")
	topic := flag.String("topic", "tracker-logs", "Topic name")
	workersCount := flag.Int("workers", 16, "Number of concurrent workers")
	flag.Parse()

	log.Printf("Starting log shipper targeting broker %s (redis: %s) on topic %s with %d workers", *brokerAddr, *redisURL, *topic, *workersCount)

	jobs := make(chan []byte, 10000)
	var wg sync.WaitGroup

	for i := 0; i < *workersCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			cli := client.NewClient(*brokerAddr, 5*time.Second)
			if *redisURL != "" {
				cli.SetRedisURL(*redisURL)
			}
			for {
				err := cli.Connect()
				if err == nil {
					break
				}
				log.Printf("[Worker %d] Failed to connect to broker, retrying in 1s: %v", workerID, err)
				time.Sleep(time.Second)
			}
			defer cli.Close()

			var count int64
			lastReport := time.Now()

			for payload := range jobs {
				_, err := cli.Produce(*topic, payload)
				if err != nil {
					log.Printf("[Worker %d] Error producing: %v", workerID, err)
				} else {
					count++
				}

				if time.Since(lastReport) > 5*time.Second {
					log.Printf("[Worker %d] Sent %d messages", workerID, count)
					lastReport = time.Now()
				}
			}
		}(i)
	}

	var file *os.File
	var err error
	for {
		file, err = os.Open(*logFilePath)
		if err == nil {
			break
		}
		log.Printf("Waiting for log file %s to be created: %v", *logFilePath, err)
		time.Sleep(time.Second)
	}
	defer file.Close()

	header := make([]byte, 4)
	payloadBuf := make([]byte, 1024*1024)

	for {
		_, err := io.ReadFull(file, header)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				stat, statErr := os.Stat(*logFilePath)
				if statErr == nil {
					currStat, fileStatErr := file.Stat()
					if fileStatErr == nil {
						if stat.Size() < currStat.Size() {
							log.Printf("Log file rotation detected (size shrunk). Reopening.")
							file.Close()
							for {
								file, err = os.Open(*logFilePath)
								if err == nil {
									break
								}
								time.Sleep(100 * time.Millisecond)
							}
							continue
						}
					}
				}
				time.Sleep(5 * time.Millisecond)
				continue
			}
			log.Printf("Error reading length header: %v", err)
			time.Sleep(time.Second)
			continue
		}

		length := binary.BigEndian.Uint32(header)
		if length == 0 {
			continue
		}

		if int(length) > len(payloadBuf) {
			payloadBuf = make([]byte, length)
		}
		_, err = io.ReadFull(file, payloadBuf[:length])
		if err != nil {
			log.Printf("Error reading payload: %v", err)
			continue
		}

		payloadCopy := make([]byte, length)
		copy(payloadCopy, payloadBuf[:length])

		select {
		case jobs <- payloadCopy:
		default:
			jobs <- payloadCopy
		}
	}
}
