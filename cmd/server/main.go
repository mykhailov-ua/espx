package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/repository"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/database"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := database.Connect(ctx, cfg.DBDSN, cfg.DBMaxConns, cfg.DBMinConns)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	queries := repository.New(pool)

	partManager := database.NewPartitionManager(pool, cfg.LogRetentionDays, 2)
	partManager.StartBackground(ctx)

	registry := ads.NewRegistry(queries)
	count, err := registry.Sync(ctx)
	if err != nil {
		slog.Warn("initial campaign registry sync failed", "error", err)
	} else {
		slog.Info("campaign registry loaded", "campaigns", count)
	}
	registry.StartSync(ctx, 1*time.Minute)

	rdb, err := database.ConnectRedis(ctx, cfg.RedisAddr)
	if err != nil {
		slog.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}
	defer rdb.Close()

	eventProc := ads.NewProcessor(
		queries,
		rdb,
		cfg.RedisStreamName,
		cfg.RedisGroupName,
		cfg.RedisConsumerID,
		cfg.EventBatchSize,
		cfg.MaxWorkers,
		time.Duration(cfg.EventFlushMs)*time.Millisecond,
		time.Duration(cfg.WriteTimeoutMs)*time.Millisecond,
	)
	eventProc.Start(ctx)

	filterEngine := ads.NewFilterEngine(
		ads.NewIPRateLimiter(rdb, cfg.RateLimitPerMin, 1*time.Minute),
		ads.NewDuplicateEventFilter(rdb, time.Duration(cfg.DuplicateTTLSec)*time.Second),
	)

	mux := ads.NewRouter(cfg, registry, eventProc, filterEngine)

	slog.Info("starting ad-event-processor", "port", cfg.ServerPort)

	server := &http.Server{
		Addr:    ":" + cfg.ServerPort,
		Handler: mux,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	sig := <-stop
	slog.Info("received shutdown signal", "signal", sig.String())

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Duration(cfg.ShutdownTimeoutMs)*time.Millisecond)
	defer shutdownCancel()

	cancel()

	slog.Info("stopping http server...")
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("server shutdown failed", "error", err)
	} else {
		slog.Info("http server stopped successfully")
	}

	slog.Info("draining event processor...")
	eventProc.Close()
	eventProc.Wait()
	slog.Info("event processor stopped")

	slog.Info("stopping campaign registry...")
	registry.Wait()
	slog.Info("campaign registry stopped")

	slog.Info("closing database pool...")
	pool.Close()
	slog.Info("graceful shutdown complete")
}
