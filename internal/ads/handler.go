package ads

import (
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/pb"
	// "github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
)

import (
	"bytes"
	"context"
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
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/mykhailov-ua/ad-event-processor/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
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
	statusStrings     [600]string
	maxPoolObjectSize = 64 * 1024 // 64KB
)

func init() {
	for i := 0; i < 600; i++ {
		statusStrings[i] = strconv.Itoa(i)
	}
}

func putBuffer(buf *bytes.Buffer) {
	if buf == nil || buf.Cap() > maxPoolObjectSize {
		return
	}
	buf.Reset()
	bufferPool.Put(buf)
}

func putAdEvent(evt *pb.AdEvent) {
	if evt == nil {
		return
	}
	if evt.Metadata != nil && len(evt.Metadata.Extra) > 100 {
		return
	}
	evt.Reset()
	adEventPool.Put(evt)
}

func putTrackResponse(resp *pb.TrackResponse) {
	if resp == nil {
		return
	}
	resp.Reset()
	trackResponsePool.Put(resp)
}

type Pinger interface {
	Ping(ctx context.Context) error
}

func NewRouter(cfg *config.Config, registry domain.CampaignRegistry, filterEngine *FilterEngine, pool Pinger, rdbs []redis.UniversalClient, sharder Sharder, fraudStream string) http.Handler {
	mux := http.NewServeMux()

	mux.Handle("GET /metrics", promhttp.Handler())

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		if err := pool.Ping(ctx); err != nil {
			slog.Error("health check failed: postgres", "error", err)
			http.Error(w, "postgres unreachable", http.StatusServiceUnavailable)
			return
		}

		for i, rdb := range rdbs {
			if err := rdb.Ping(ctx).Err(); err != nil {
				slog.Error("health check failed: redis shard", "shard", i, "error", err)
				http.Error(w, "redis shard unreachable", http.StatusServiceUnavailable)
				return
			}
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
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
			statusStr := "500"
			if status >= 0 && status < len(statusStrings) {
				statusStr = statusStrings[status]
			}
			metrics.HttpRequestsTotal.WithLabelValues("POST", "/track", statusStr).Inc()
			metrics.HttpRequestDuration.WithLabelValues("POST", "/track").Observe(duration)
		}()

		r.Body = http.MaxBytesReader(w, r.Body, cfg.MaxRequestBodySize)

		id, _ := uuid.NewV7()
		requestID := id.String()

		var campaignID uuid.UUID
		var eventType string
		var userID string
		var payload []byte

		ip := extractClientIP(r)
		clickID := requestID

		contentType := r.Header.Get("Content-Type")
		if contentType == "application/x-protobuf" || contentType == "" {
			buf := bufferPool.Get().(*bytes.Buffer)
			defer putBuffer(buf)

			if _, err := io.Copy(buf, r.Body); err != nil {
				slog.Warn("failed to read body", "error", err, "request_id", id)
				status = http.StatusBadRequest
				http.Error(w, "invalid body", status)
				return
			}

			pbReq := adEventPool.Get().(*pb.AdEvent)
			defer putAdEvent(pbReq)

			if err := proto.Unmarshal(buf.Bytes(), pbReq); err != nil {
				slog.Warn("invalid protobuf body", "error", err, "request_id", id)
				status = http.StatusBadRequest
				http.Error(w, "invalid protobuf", status)
				return
			}

			cid, err := uuid.Parse(pbReq.CampaignId)
			if err != nil {
				slog.Warn("invalid campaign id in proto", "error", err, "request_id", id)
				status = http.StatusBadRequest
				http.Error(w, "invalid campaign_id", status)
				return
			}
			campaignID = cid
			eventType = pbReq.EventType
			if pbReq.Metadata != nil {
				userID = pbReq.Metadata.UserId
				if pbReq.Metadata.ClickId != "" {
					clickID = pbReq.Metadata.ClickId
				}
				if pbReq.Metadata.Extra != nil {
					var err error
					payload, err = json.Marshal(pbReq.Metadata.Extra)
					if err != nil {
						slog.Warn("failed to marshal extra metadata", "error", err, "request_id", id)
					}
				}
			}
		} else {
			buf := bufferPool.Get().(*bytes.Buffer)
			defer putBuffer(buf)

			if _, err := io.Copy(buf, r.Body); err != nil {
				slog.Warn("failed to read body", "error", err, "request_id", id)
				status = http.StatusBadRequest
				http.Error(w, "invalid body", status)
				return
			}

			var req struct {
				CampaignID uuid.UUID       `json:"campaign_id"`
				UserID     string          `json:"user_id"`
				Type       string          `json:"type"`
				ClickID    string          `json:"click_id"`
				Payload    json.RawMessage `json:"payload"`
			}
			if err := json.Unmarshal(buf.Bytes(), &req); err != nil {
				slog.Warn("invalid json body", "error", err, "request_id", id)
				status = http.StatusBadRequest
				http.Error(w, "invalid json", status)
				return
			}
			campaignID = req.CampaignID
			userID = req.UserID
			eventType = req.Type
			payload = req.Payload
			if req.ClickID != "" {
				clickID = req.ClickID
			}
		}

		evt := domain.EventPool.Get().(*domain.Event)
		evt.Reset()
		evt.ClickID = clickID
		evt.CampaignID = campaignID
		evt.UserID = userID
		evt.Type = eventType
		evt.Payload = append(evt.Payload[:0], payload...)
		evt.IP = ip
		evt.UA = r.UserAgent()

		if filterEngine != nil {
			if err := filterEngine.Check(r.Context(), evt); err != nil {
				if errors.Is(err, ErrRateLimitExceeded) {
					domain.EventPool.Put(evt)
					slog.Warn("event rejected: rate limit", "error", err, "request_id", id)
					metrics.FilterBlockedTotal.WithLabelValues("rate_limit").Inc()
					http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
					return
				} else if errors.Is(err, ErrDuplicateEvent) {
					domain.EventPool.Put(evt)
					slog.Warn("event rejected: duplicate", "error", err, "request_id", id)
					metrics.FilterBlockedTotal.WithLabelValues("duplicate").Inc()
					http.Error(w, "duplicate event", http.StatusConflict)
					return
				} else if errors.Is(err, ErrBudgetExhausted) {
					domain.EventPool.Put(evt)
					slog.Warn("event rejected: budget exhausted", "error", err, "request_id", id)
					metrics.FilterBlockedTotal.WithLabelValues("budget").Inc()
					http.Error(w, "budget exhausted", http.StatusPaymentRequired)
					return
				} else if errors.Is(err, ErrPacingExhausted) {
					domain.EventPool.Put(evt)
					slog.Warn("event rejected: pacing exhausted", "error", err, "request_id", id)
					metrics.FilterBlockedTotal.WithLabelValues("pacing").Inc()
					http.Error(w, "pacing limit reached", http.StatusTooManyRequests)
					return
				} else if errors.Is(err, ErrFreqLimitExceeded) {
					domain.EventPool.Put(evt)
					slog.Warn("event rejected: frequency limit", "error", err, "request_id", id)
					metrics.FilterBlockedTotal.WithLabelValues("freq").Inc()
					http.Error(w, "frequency limit reached", http.StatusForbidden)
					return
				} else if errors.Is(err, ErrGeoBlocked) {
					domain.EventPool.Put(evt)
					slog.Warn("event rejected: geo blocked", "error", err, "request_id", id)
					metrics.FilterBlockedTotal.WithLabelValues("geo").Inc()
					http.Error(w, "geo-targeting blocked", http.StatusForbidden)
					return
				} else if errors.Is(err, ErrFraudDetected) {
					slog.Warn("fraud detected: silent drop", "reason", evt.FraudReason, "request_id", id)
					metrics.FilterBlockedTotal.WithLabelValues("fraud").Inc()

					shard := sharder.GetShard(evt.CampaignID)
					rdb := rdbs[shard]

					if err := rdb.XAdd(r.Context(), &redis.XAddArgs{
						Stream: fraudStream,
						MaxLen: int64(cfg.StreamMaxLen),
						Approx: true,
						Values: map[string]interface{}{
							"click_id":     evt.ClickID,
							"campaign_id":  evt.CampaignID.String(),
							"user_id":      evt.UserID,
							"type":         evt.Type,
							"ip":           evt.IP,
							"ua":           evt.UA,
							"payload":      string(evt.Payload),
							"fraud_reason": evt.FraudReason,
							"created_at":   evt.CreatedAt.Format(time.RFC3339Nano),
						},
					}).Err(); err != nil {
						slog.Error("failed to write to fraud stream", "error", err, "request_id", id)
					}

					// Silent drop
				} else {
					domain.EventPool.Put(evt)
					slog.Error("filter engine failure", "error", err, "request_id", id)
					http.Error(w, "internal error", http.StatusInternalServerError)
					return
				}
			}
		}

		domain.EventPool.Put(evt)

		if r.Header.Get("Accept") == "application/x-protobuf" {
			resp := trackResponsePool.Get().(*pb.TrackResponse)
			defer putTrackResponse(resp)

			resp.RequestId = requestID
			resp.Status = "accepted"

			buf := bufferPool.Get().(*bytes.Buffer)
			defer putBuffer(buf)

			out, err := proto.MarshalOptions{}.MarshalAppend(buf.Bytes(), resp)
			if err != nil {
				slog.Error("failed to marshal proto response", "error", err, "request_id", id)
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/x-protobuf")
			w.WriteHeader(status)
			w.Write(out)
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)

			buf := bufferPool.Get().(*bytes.Buffer)
			defer putBuffer(buf)

			buf.WriteString(`{"request_id":"`)
			buf.WriteString(requestID)
			buf.WriteString(`","status":"accepted"}`)
			w.Write(buf.Bytes())
		}
	})

	return mux
}

func extractClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		last := len(xff)
		for i := len(xff) - 1; i >= -1; i-- {
			if i == -1 || xff[i] == ',' {
				start := i + 1
				for start < last && xff[start] == ' ' {
					start++
				}
				end := last
				for end > start && xff[end-1] == ' ' {
					end--
				}

				if start < end {
					ipStr := xff[start:end]
					parsedIP := net.ParseIP(ipStr)
					if parsedIP != nil && !parsedIP.IsPrivate() && !parsedIP.IsLoopback() && !parsedIP.IsLinkLocalUnicast() {
						return ipStr
					}
				}
				last = i
			}
		}
	}

	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		ipStr := strings.TrimSpace(xri)
		parsedIP := net.ParseIP(ipStr)
		if parsedIP != nil && !parsedIP.IsPrivate() && !parsedIP.IsLoopback() && !parsedIP.IsLinkLocalUnicast() {
			return ipStr
		}
	}

	remoteIP, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return remoteIP
}
