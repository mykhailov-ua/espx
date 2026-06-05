package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"fmt"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/database"
	"github.com/mykhailov-ua/ad-event-processor/pkg/logger"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func main() {
	if len(os.Args) > 2 && os.Args[1] == "--health-probe" {
		resp, err := http.Get(os.Args[2])
		if err != nil || resp.StatusCode != 200 {
			os.Exit(1)
		}
		os.Exit(0)
	}

	slogLogger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(slogLogger)

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	loggerCfg := logger.Config{
		LogDir:                cfg.Logger.Dir,
		FlushBufferSize:       cfg.Logger.FlushSizeKB * 1024,
		RotateSize:            int64(cfg.Logger.RotateSizeMB) * 1024 * 1024,
		RotateInterval:        cfg.Logger.RotateInterval,
		DiskLatencyLimit:      cfg.Logger.LatencyLimit,
		PersistQueueDepth:     cfg.Logger.PersistQueueDepth,
		PersistEnqueueTimeout: cfg.Logger.PersistEnqueueTimeout,
	}
	appLogger := logger.NewLogger(loggerCfg, cfg.Logger.Shards)
	defer appLogger.Close()

	logger.RegisterMetrics()
	appLogger.StartMetricsReporter(15 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	procCtx, procCancel := context.WithCancel(context.Background())
	defer procCancel()

	pool, err := database.Connect(ctx, string(cfg.DBDSN), cfg.DBProcessorMaxConns, cfg.DBMinConns)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	queries := db.New(pool)
	partManager := database.NewPartitionManager(pool, cfg.LogRetentionDays, cfg.PartitionPreCreateDays)
	partManager.StartBackground(ctx)

	chConn, err := database.ConnectClickHouse(ctx, string(cfg.CHDSN))
	if err != nil {
		slog.Error("failed to connect to clickhouse", "error", err)
		os.Exit(1)
	}
	defer chConn.Close()

	var rdbs []redis.UniversalClient
	for _, addr := range cfg.RedisAddrs {
		rdb := redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs:    []string{addr},
			Password: string(cfg.RedisPassword),
			PoolSize: cfg.RedisPoolSize,
		})

		var rdbErr error
		for i := 0; i < 30; i++ {
			if rdbErr = rdb.Ping(ctx).Err(); rdbErr == nil {
				break
			}
			slog.Warn("waiting for redis...", "addr", addr, "error", rdbErr)
			time.Sleep(time.Second)
		}

		if rdbErr != nil {
			slog.Error("failed to connect to redis shard", "addr", addr, "error", rdbErr)
			os.Exit(1)
		}
		breaker := database.NewRedisBreaker(50, 3, 5*time.Second)
		rdb.AddHook(database.NewRedisCircuitBreakerHook(breaker))
		rdbs = append(rdbs, rdb)
	}

	pgStore := ads.NewPostgresStore(queries, time.Duration(cfg.WriteTimeoutMs)*time.Millisecond)
	chStore := ads.NewClickHouseStore(chConn, time.Duration(cfg.WriteTimeoutMs)*time.Millisecond)

	campaignRepo := ads.NewCampaignRepo(queries)
	customerRepo := ads.NewCustomerRepo(queries)

	var pgConsumers []*ads.StreamConsumer
	var chConsumers []*ads.StreamConsumer
	var syncWorkers []*ads.SyncWorker

	for i, rdb := range rdbs {
		shardID := fmt.Sprintf("shard_%d", i)

		sw := ads.NewSyncWorker(rdb, campaignRepo, customerRepo, time.Duration(cfg.BudgetSyncIntervalMs)*time.Millisecond)
		syncWorkers = append(syncWorkers, sw)
		sw.Start(procCtx)

		pc := ads.NewStreamConsumer(
			pgStore,
			rdb,
			cfg.RedisStreamName,
			cfg.RedisGroupName+"_pg",
			cfg.RedisConsumerID+"_"+shardID,
			cfg.EventBatchSize,
			cfg.MaxWorkers,
			time.Duration(cfg.EventFlushMs)*time.Millisecond,
			time.Duration(cfg.WriteTimeoutMs)*time.Millisecond,
			time.Duration(cfg.RetryInitialWaitMs)*time.Millisecond,
			time.Duration(cfg.RetryMaxWaitMs)*time.Millisecond,
			cfg.MaxRetries,
			time.Duration(cfg.StreamMinIdleMs)*time.Millisecond,
			time.Duration(cfg.Lifecycle.DrainTimeoutMs)*time.Millisecond,
		)
		pc.SetLogger(appLogger)
		pgConsumers = append(pgConsumers, pc)
		pc.Start(procCtx)

		cc := ads.NewStreamConsumer(
			chStore,
			rdb,
			cfg.RedisStreamName,
			cfg.RedisGroupName+"_ch",
			cfg.RedisConsumerID+"_"+shardID,
			cfg.CHBatchSize,
			cfg.CHMaxWorkers,
			time.Duration(cfg.CHFlushIntervalMs)*time.Millisecond,
			time.Duration(cfg.WriteTimeoutMs)*time.Millisecond,
			time.Duration(cfg.RetryInitialWaitMs)*time.Millisecond,
			time.Duration(cfg.RetryMaxWaitMs)*time.Millisecond,
			cfg.MaxRetries,
			time.Duration(cfg.StreamMinIdleMs)*time.Millisecond,
			time.Duration(cfg.Lifecycle.DrainTimeoutMs)*time.Millisecond,
		)
		cc.SetLogger(appLogger)
		chConsumers = append(chConsumers, cc)
		cc.Start(procCtx)

		fc := ads.NewStreamConsumer(
			chStore,
			rdb,
			cfg.FraudStreamName,
			cfg.RedisGroupName+"_fraud",
			cfg.RedisConsumerID+"_fraud_"+shardID,
			cfg.CHBatchSize,
			cfg.CHMaxWorkers,
			time.Duration(cfg.CHFlushIntervalMs)*time.Millisecond,
			time.Duration(cfg.WriteTimeoutMs)*time.Millisecond,
			time.Duration(cfg.RetryInitialWaitMs)*time.Millisecond,
			time.Duration(cfg.RetryMaxWaitMs)*time.Millisecond,
			cfg.MaxRetries,
			time.Duration(cfg.StreamMinIdleMs)*time.Millisecond,
			time.Duration(cfg.Lifecycle.DrainTimeoutMs)*time.Millisecond,
		)
		fc.SetLogger(appLogger)
		chConsumers = append(chConsumers, fc)
		fc.Start(procCtx)
	}

	slog.Info("starting ad-event-processor worker",
		"stream", cfg.RedisStreamName,
		"pg_group", cfg.RedisGroupName+"_pg",
		"ch_group", cfg.RedisGroupName+"_ch",
		"port", cfg.ProcessorPort,
	)

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		if err := pool.Ping(ctx); err != nil {
			slog.Error("processor health check failed: postgres", "error", err)
			http.Error(w, "postgres unreachable", http.StatusServiceUnavailable)
			return
		}

		if err := chConn.Ping(ctx); err != nil {
			slog.Error("processor health check failed: clickhouse", "error", err)
			http.Error(w, "clickhouse unreachable", http.StatusServiceUnavailable)
			return
		}

		for i, rdb := range rdbs {
			if err := rdb.Ping(ctx).Err(); err != nil {
				slog.Error("processor health check failed: redis shard", "shard", i, "error", err)
				http.Error(w, "redis shard unreachable", http.StatusServiceUnavailable)
				return
			}
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	server := &http.Server{
		Addr:    ":" + cfg.ProcessorPort,
		Handler: mux,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("processor http server failed", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	slog.Info("shutting down processor")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Lifecycle.ShutdownTimeoutMs)*time.Millisecond)
	defer shutdownCancel()

	procCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("processor server shutdown failed", "error", err)
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Lifecycle.WaitTimeoutMs)*time.Millisecond)
	defer waitCancel()

	for _, pc := range pgConsumers {
		pc.Close()
		if err := pc.Wait(waitCtx); err != nil {
			slog.Error("pg consumer wait failed", "error", err)
		}
	}
	pgStore.Close()

	for _, cc := range chConsumers {
		cc.Close()
		if err := cc.Wait(waitCtx); err != nil {
			slog.Error("ch consumer wait failed", "error", err)
		}
	}
	chStore.Close()

	for i, sw := range syncWorkers {
		if err := sw.Wait(waitCtx); err != nil {
			slog.Error("sync worker wait failed", "shard", i, "error", err)
		}
	}

	if err := partManager.Wait(waitCtx); err != nil {
		slog.Error("partition manager wait failed", "error", err)
	}

	cancel()

	for i, rdb := range rdbs {
		if err := rdb.Close(); err != nil {
			slog.Error("failed to close redis shard", "shard", i, "error", err)
		}
	}
	slog.Info("processor shutdown complete")
}
