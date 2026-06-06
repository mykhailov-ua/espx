package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/mykhailov-ua/ad-event-processor/pkg/broker/server"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9092", "Address for gnet TCP traffic")
	healthAddr := flag.String("health-addr", "127.0.0.1:8081", "Address for HTTP health checks")
	dataDir := flag.String("data-dir", "/tmp/espx-broker", "Data directory for segments")
	nodeID := flag.String("node-id", "broker-1", "Unique node ID")
	redisURL := flag.String("redis-url", "redis://127.0.0.1:6379/0", "Redis URL for coordination")
	maxSegSize := flag.Int64("max-seg-size", 64*1024*1024, "Maximum segment size in bytes")
	indexInterval := flag.Int64("index-interval", 4096, "Index interval in bytes")
	flag.Parse()

	slog.Info("Starting ESPX Broker", "node_id", *nodeID, "addr", *addr, "health_addr", *healthAddr)

	srv := server.NewServer(*addr, *dataDir, *maxSegSize, *indexInterval)
	srv.SetHealthAddr(*healthAddr)

	if err := srv.Start(); err != nil {
		slog.Error("Failed to start server", "error", err)
		os.Exit(1)
	}

	coord, err := server.NewCoordinator(*nodeID, srv.Addr(), *redisURL, srv)
	if err != nil {
		slog.Error("Failed to initialize coordinator", "error", err)
		srv.Stop()
		os.Exit(1)
	}

	srv.SetCoordinator(coord)
	coord.Start()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	slog.Info("ESPX Broker running. Press Ctrl+C to exit.")
	<-sigChan

	slog.Info("Shutting down ESPX Broker...")
	coord.Stop()
	srv.Stop()
	slog.Info("Shutdown complete.")
}
