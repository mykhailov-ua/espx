package ads

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/pb"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/protobuf/proto"
)

var (
	adEventPool = sync.Pool{
		New: func() any { return &pb.AdEvent{} },
	}
	trackResponsePool = sync.Pool{
		New: func() any { return &pb.TrackResponse{} },
	}
	bufferPool = sync.Pool{
		New: func() any { return new(bytes.Buffer) },
	}
)

func NewRouter(cfg *config.Config, registry domain.CampaignRegistry, proc *StreamConsumer, filterEngine *FilterEngine) http.Handler {
	mux := http.NewServeMux()

	mux.Handle("GET /metrics", promhttp.Handler())

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

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

		// Prevent OOM by limiting request body size to 1MB
		r.Body = http.MaxBytesReader(w, r.Body, cfg.MaxRequestBodySize)

		requestID := uuid.New().String()
		l := slog.With("request_id", requestID)

		var campaignID uuid.UUID
		var eventType string
		var payload []byte

		ip := extractClientIP(r)
		clickID := requestID

		contentType := r.Header.Get("Content-Type")
		if contentType == "application/x-protobuf" {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				l.Warn("failed to read body", "error", err)
				status = http.StatusBadRequest
				http.Error(w, "invalid body", status)
				return
			}

			pbReq := adEventPool.Get().(*pb.AdEvent)
			pbReq.Reset()
			defer adEventPool.Put(pbReq)

			if err := proto.Unmarshal(body, pbReq); err != nil {
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
				buf := bufferPool.Get().(*bytes.Buffer)
				buf.Reset()
				defer func() {
					if buf.Cap() <= 64*1024 {
						bufferPool.Put(buf)
					}
				}()

				enc := json.NewEncoder(buf)
				_ = enc.Encode(pbReq.Metadata)
				payload = make([]byte, buf.Len())
				copy(payload, buf.Bytes())
			}
		} else {
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

		evt := domain.EventPool.Get().(*domain.Event)
		evt.Reset()
		evt.ClickID = clickID
		evt.CampaignID = campaignID
		evt.Type = eventType
		evt.Payload = append(evt.Payload[:0], payload...)
		evt.IP = ip
		evt.UA = r.UserAgent()

		if filterEngine != nil {
			if err := filterEngine.Check(r.Context(), evt); err != nil {
				if errors.Is(err, ErrRateLimitExceeded) {
					l.Warn("event rejected: rate limit", "error", err)
					http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
					return
				} else if errors.Is(err, ErrDuplicateEvent) {
					l.Warn("event rejected: duplicate", "error", err)
					http.Error(w, "duplicate event", http.StatusConflict)
					return
				} else if errors.Is(err, ErrBudgetExhausted) {
					l.Warn("event rejected: budget exhausted", "error", err)
					http.Error(w, "budget exhausted", http.StatusPaymentRequired)
					return
				}

				// Fail-open for infrastructure errors (e.g., Redis down)
				l.Error("filter engine degraded (fail-open)", "error", err)
			}
		}

		err := proc.Process(evt)
		domain.EventPool.Put(evt)

		if err != nil {
			l.Error("failed to process event", "error", err)
			status = http.StatusInternalServerError
			http.Error(w, "internal error", status)
			return
		}

		if r.Header.Get("Accept") == "application/x-protobuf" {
			resp := trackResponsePool.Get().(*pb.TrackResponse)
			resp.Reset()
			defer trackResponsePool.Put(resp)

			resp.RequestId = requestID
			resp.Status = "accepted"

			out, _ := proto.Marshal(resp)
			w.Header().Set("Content-Type", "application/x-protobuf")
			w.WriteHeader(status)
			_, _ = w.Write(out)
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)

			buf := bufferPool.Get().(*bytes.Buffer)
			buf.Reset()
			defer func() {
				if buf.Cap() <= 64*1024 {
					bufferPool.Put(buf)
				}
			}()

			_, _ = buf.WriteString(`{"request_id":"`)
			_, _ = buf.WriteString(requestID)
			_, _ = buf.WriteString(`","status":"accepted"}`)
			_, _ = w.Write(buf.Bytes())
		}
	})

	return mux
}

func extractClientIP(r *http.Request) string {
	remoteIP, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		remoteIP = r.RemoteAddr
	}

	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Optimization: avoid strings.Split. Parse from right to left to find first non-private IP.
		last := len(xff)
		for i := len(xff) - 1; i >= -1; i-- {
			if i == -1 || xff[i] == ',' {
				ipStr := strings.TrimSpace(xff[i+1 : last])
				parsedIP := net.ParseIP(ipStr)
				if parsedIP != nil && !parsedIP.IsPrivate() && !parsedIP.IsLoopback() && !parsedIP.IsLinkLocalUnicast() {
					return ipStr
				}
				last = i
			}
		}
	} else if xri := r.Header.Get("X-Real-IP"); xri != "" {
		ipStr := strings.TrimSpace(xri)
		parsedIP := net.ParseIP(ipStr)
		if parsedIP != nil && !parsedIP.IsPrivate() && !parsedIP.IsLoopback() && !parsedIP.IsLinkLocalUnicast() {
			return ipStr
		}
	}

	return remoteIP
}
