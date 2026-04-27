package main

import (
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"log/slog"
	"net/http"
	"os"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	slog.Info("starting ad-event-processor", 
		"port", cfg.ServerPort,
		"db_configured", cfg.DBDSN != "",
		"redis_configured", cfg.RedisAddr != "",
	)

	if err := http.ListenAndServe(":"+cfg.ServerPort, mux); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}
