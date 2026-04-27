package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/database"
	"github.com/mykhailov-ua/ad-event-processor/internal/database/db"
	"github.com/mykhailov-ua/ad-event-processor/internal/event"
	"github.com/mykhailov-ua/ad-event-processor/internal/stats"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// 1. Database connection
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := database.Connect(ctx, cfg.DBDSN)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	// 2. Initialize repository and workers
	queries := db.New(pool)
	
	// Batch events: 1000 events or 500ms
	eventProc := event.NewProcessor(pool, 1000, 500*time.Millisecond)
	eventProc.Start(ctx)

	// Aggregate stats: flush every 5s
	statsAgg := stats.NewAggregator(queries, 5*time.Second)
	statsAgg.Start(ctx)

	// 3. Setup router
	mux := http.NewServeMux()
	
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	mux.HandleFunc("POST /track", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CampaignID uuid.UUID       `json:"campaign_id"`
			Type       string          `json:"type"`
			Payload    json.RawMessage `json:"payload"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}

		// Fast path: push to in-memory buffers
		err := eventProc.Process(event.Event{
			CampaignID: req.CampaignID,
			Type:       req.Type,
			Payload:    req.Payload,
			IP:         r.RemoteAddr,
			UA:         r.UserAgent(),
		})

		if err != nil {
			if errors.Is(err, event.ErrBufferFull) {
				http.Error(w, "server overloaded", http.StatusTooManyRequests)
				return
			}
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		statsAgg.Increment(req.CampaignID, req.Type)

		w.WriteHeader(http.StatusAccepted)
	})

	slog.Info("starting ad-event-processor", "port", cfg.ServerPort)

	// 4. Graceful Shutdown
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

	// Wait for interrupt signal
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	slog.Info("shutting down server...")
	
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("server shutdown failed", "error", err)
	}
	
	cancel() // Stop workers and flush buffers
	time.Sleep(1 * time.Second) // Small delay to let workers finish flushing
	slog.Info("server stopped")
}
