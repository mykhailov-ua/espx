package server

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/campaign"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/event"
	"github.com/mykhailov-ua/ad-event-processor/internal/stats"
)

func NewRouter(cfg *config.Config, registry *campaign.Registry, proc *event.Processor, agg *stats.Aggregator) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	mux.HandleFunc("POST /track", func(w http.ResponseWriter, r *http.Request) {
		requestID := uuid.New().String()
		l := slog.With("request_id", requestID)

		var req struct {
			CampaignID uuid.UUID       `json:"campaign_id"`
			Type       string          `json:"type"`
			Payload    json.RawMessage `json:"payload"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			l.Warn("invalid request body", "error", err)
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}

		if !registry.Exists(req.CampaignID) {
			l.Warn("campaign not found", "campaign_id", req.CampaignID)
			http.Error(w, "campaign not found", http.StatusNotFound)
			return
		}

		err := proc.Process(event.Event{
			CampaignID: req.CampaignID,
			Type:       req.Type,
			Payload:    req.Payload,
			IP:         r.RemoteAddr,
			UA:         r.UserAgent(),
		})

		if err != nil {
			if errors.Is(err, event.ErrBufferFull) {
				l.Error("processor buffer full")
				http.Error(w, "server overloaded", http.StatusTooManyRequests)
				return
			}
			l.Error("failed to process event", "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		agg.Increment(req.CampaignID, req.Type)
		w.WriteHeader(http.StatusAccepted)
	})

	return mux
}
