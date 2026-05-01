package ads

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/pb"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/protobuf/proto"
)

// NewRouter initializes the HTTP router with metrics, health checks, and tracking endpoints.
func NewRouter(cfg *config.Config, registry *Registry, proc *Processor, agg *Aggregator) http.Handler {
	mux := http.NewServeMux()

	mux.Handle("GET /metrics", promhttp.Handler())

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	// Debugging & Profiling (pprof)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	mux.HandleFunc("POST /track", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		status := http.StatusAccepted

		defer func() {
			duration := time.Since(start).Seconds()
			HttpRequestsTotal.WithLabelValues("POST", "/track", strconv.Itoa(status)).Inc()
			HttpRequestDuration.WithLabelValues("POST", "/track").Observe(duration)
		}()

		requestID := uuid.New().String()
		l := slog.With("request_id", requestID)

		var campaignID uuid.UUID
		var eventType string
		var payload []byte

		clickID := requestID // Default to requestID for idempotency if click_id is missing

		// Protocol negotiation: handle high-performance Protobuf or debug-friendly JSON.
		contentType := r.Header.Get("Content-Type")
		if contentType == "application/x-protobuf" {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				l.Warn("failed to read body", "error", err)
				status = http.StatusBadRequest
				http.Error(w, "invalid body", status)
				return
			}
			var pbReq pb.AdEvent
			if err := proto.Unmarshal(body, &pbReq); err != nil {
				l.Warn("invalid protobuf body", "error", err)
				status = http.StatusBadRequest
				http.Error(w, "invalid protobuf", status)
				return
			}

			cid, err := uuid.Parse(pbReq.CampaignId)
			if err != nil {
				l.Warn("invalid campaign id in proto", "error", err)
				status = http.StatusBadRequest
				http.Error(w, "invalid campaign_id", status)
				return
			}
			campaignID = cid
			eventType = pbReq.EventType
			if pbReq.Metadata != nil {
				if pbReq.Metadata.ClickId != "" {
					clickID = pbReq.Metadata.ClickId
				}
				payload, _ = json.Marshal(pbReq.Metadata)
			}
		} else {
			// Fallback to JSON
			var req struct {
				CampaignID uuid.UUID       `json:"campaign_id"`
				Type       string          `json:"type"`
				ClickID    string          `json:"click_id"`
				Payload    json.RawMessage `json:"payload"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				l.Warn("invalid json body", "error", err)
				status = http.StatusBadRequest
				http.Error(w, "invalid json", status)
				return
			}
			campaignID = req.CampaignID
			eventType = req.Type
			payload = req.Payload
			if req.ClickID != "" {
				clickID = req.ClickID
			}
		}

		// Hot-path validation using in-memory registry to avoid DB Foreign Key overhead.
		if !registry.Exists(campaignID) {
			l.Warn("campaign not found", "campaign_id", campaignID)
			status = http.StatusNotFound
			http.Error(w, "campaign not found", status)
			return
		}

		err := proc.Process(Event{
			ClickID:    clickID,
			CampaignID: campaignID,
			Type:       eventType,
			Payload:    payload,
			IP:         r.RemoteAddr,
			UA:         r.UserAgent(),
		})

		if err != nil {
			l.Error("failed to process event", "error", err)
			status = http.StatusInternalServerError
			http.Error(w, "internal error", status)
			return
		}

		agg.Increment(campaignID, eventType)

		// Respond in the requested format (Protobuf or JSON).
		if r.Header.Get("Accept") == "application/x-protobuf" {
			resp := &pb.TrackResponse{
				RequestId: requestID,
				Status:    "accepted",
			}
			out, _ := proto.Marshal(resp)
			w.Header().Set("Content-Type", "application/x-protobuf")
			w.WriteHeader(status)
			_, _ = w.Write(out)
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"request_id": requestID,
				"status":     "accepted",
			})
		}
	})

	return mux
}
