// Package ads implements the tracker HTTP layer responsible for receiving, validating,
// and routing ad-event signals (impressions, clicks, conversions) into the Redis Stream
// ingestion pipeline. Two server backends co-exist: a standard net/http mux (NewRouter)
// used by the test harness and the management plane, and a lock-free gnet event-loop
// handler (AdsPacketHandler) used in production. The gnet path bypasses the Go HTTP
// server entirely - inbound TCP data is parsed with a zero-alloc hand-written HTTP/1.1
// parser, event state is stored in connection-scoped connContext structs attached to
// each gnet.Conn, and responses are written as pre-built byte literals or assembled
// in-place into per-connection slices to avoid the heap pressure of http.ResponseWriter.
//
// All allocation-heavy objects (pb.AdEvent, pb.TrackResponse, TrackRequest, fraud value
// slices, response byte buffers) are managed via sync.Pool with double-indirection
// pointer pattern (*[]byte, *[]any) to prevent per-call allocations on the hot path.
// See docs/architecture.md Tracker section and docs/development.md Pool conventions section.
package ads

import (
	"espx/internal/ads/pb"
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
	"sync/atomic"
	"time"

	"espx/internal/config"
	"espx/internal/domain"
	"espx/internal/metrics"
	"espx/pkg/logger"
	"github.com/google/uuid"
	"github.com/panjf2000/gnet/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/expfmt"
	"github.com/redis/go-redis/v9"
)

var (
	adEventPool = sync.Pool{
		New: func() any {
			return &pb.AdEvent{
				Metadata: &pb.EventMetadata{},
			}
		},
	}
	trackResponsePool = sync.Pool{
		New: func() any { return &pb.TrackResponse{} },
	}
	trackRequestPool = sync.Pool{
		New: func() any {
			return &TrackRequest{
				Payload: make([]byte, 0, 512),
			}
		},
	}
	bufferPool = sync.Pool{
		New: func() any { return new(bytes.Buffer) },
	}
	fraudValuesPool = sync.Pool{
		New: func() any {
			s := make([]any, 18)
			return &s
		},
	}
	responseBytesPool = sync.Pool{
		New: func() any {
			s := make([]byte, 4096)
			return &s
		},
	}
	statusStrings          [600]string
	maxPoolObjectSize      = 64 * 1024
	contentTypeProtoHeader = []string{"application/x-protobuf"}
	contentTypeJsonHeader  = []string{"application/json"}
)

// connContext holds per-connection working memory for the gnet event-loop path.
// By embedding all intermediate buffers and protobuf structs in this struct and
// attaching it to gnet.Conn via SetContext, successive requests on the same keep-alive
// connection reuse the same allocations with no GC pressure. Fields prefixed with 'w'
// are bufWrapper instances pooled from bufPool; they serve as staging areas for UUID
// and timestamp string construction without escaping to the heap.
type connContext struct {
	pbReq      pb.AdEvent
	reqJSON    TrackRequest
	evt        domain.Event
	valSlice   []any
	resp       pb.TrackResponse
	bufSlice   []byte
	wReqID     bufWrapper
	wCamp      bufWrapper
	wTime      bufWrapper
	bufMetrics bytes.Buffer
	remoteIP   string
	shardID    int
}

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
	if evt.Metadata != nil && (len(evt.Metadata.Extra) > 100 || cap(evt.Metadata.ExtraBytes) > 4096) {
		evt.Reset()
		adEventPool.Put(evt)
		return
	}
	evt.CampaignId = evt.CampaignId[:0]
	evt.EventType = evt.EventType[:0]
	if evt.Metadata != nil {
		evt.Metadata.ClickId = evt.Metadata.ClickId[:0]
		evt.Metadata.UserId = evt.Metadata.UserId[:0]
		evt.Metadata.DeviceType = evt.Metadata.DeviceType[:0]
		evt.Metadata.Os = evt.Metadata.Os[:0]
		for k := range evt.Metadata.Extra {
			delete(evt.Metadata.Extra, k)
		}
		evt.Metadata.ExtraBytes = evt.Metadata.ExtraBytes[:0]
	}
	adEventPool.Put(evt)
}

func putTrackResponse(resp *pb.TrackResponse) {
	if resp == nil {
		return
	}
	resp.Reset()
	trackResponsePool.Put(resp)
}

// Pinger abstracts the health-check surface of a connection pool or client.
// Implemented by *pgxpool.Pool and redis.UniversalClient; separated to allow
// testcontainers-based integration tests to substitute lightweight fakes.
type Pinger interface {
	Ping(ctx context.Context) error
}

// NewRouter constructs the standard net/http ServeMux used by the test harness and
// management tooling. The /track handler path allocates per-request objects from
// sync.Pool and calls the same FilterEngine pipeline as the gnet path, but uses
// io.Copy + bufferPool instead of zero-copy Peek because net/http does not expose
// the raw TCP ring buffer. Status counter pre-allocation (600-element fixed array)
// eliminates the WithLabelValues allocation on every response write.
func NewRouter(cfg *config.Config, registry domain.CampaignRegistry, filterEngine *FilterEngine, pool Pinger, rdbs []redis.UniversalClient, sharder Sharder, fraudStream string) http.Handler {
	mux := http.NewServeMux()

	trackDurationObserver := metrics.HttpRequestDuration.WithLabelValues("POST", "/track")
	var trackStatusCounters [600]prometheus.Counter
	for i := 0; i < 600; i++ {
		trackStatusCounters[i] = metrics.HttpRequestsTotal.WithLabelValues("POST", "/track", statusStrings[i])
	}

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
			if status >= 0 && status < 600 {
				trackStatusCounters[status].Inc()
			} else {
				metrics.HttpRequestsTotal.WithLabelValues("POST", "/track", strconv.Itoa(status)).Inc()
			}
			trackDurationObserver.Observe(duration)
		}()

		if r.ContentLength > cfg.MaxRequestBodySize {
			slog.Warn("request body size exceeds limit", "size", r.ContentLength, "limit", cfg.MaxRequestBodySize)
			status = http.StatusBadRequest
			http.Error(w, "invalid body", status)
			return
		}
		if r.ContentLength < 0 {
			r.Body = http.MaxBytesReader(w, r.Body, cfg.MaxRequestBodySize)
		}

		id, _ := NewFastUUID()
		wReqID := bufPool.Get().(*bufWrapper)
		wReqID.buf = wReqID.buf[:0]
		wReqID.buf = appendUUID(wReqID.buf, id)
		defer bufPool.Put(wReqID)

		var campaignID uuid.UUID
		var eventType string
		var userID string
		var payload []byte

		ip := extractClientIP(r, cfg.TrustedProxies)
		var clickID string
		var requestIDStr string

		contentType := ""
		if ctSlice := r.Header["Content-Type"]; len(ctSlice) > 0 {
			contentType = ctSlice[0]
		}
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

			if err := pbReq.UnmarshalVT(buf.Bytes()); err != nil {
				slog.Warn("invalid protobuf body", "error", err, "request_id", id)
				status = http.StatusBadRequest
				http.Error(w, "invalid protobuf", status)
				return
			}

			cid, err := uuid.ParseBytes(pbReq.CampaignId)
			if err != nil {
				slog.Warn("invalid campaign id in proto", "error", err, "request_id", id)
				status = http.StatusBadRequest
				http.Error(w, "invalid campaign_id", status)
				return
			}
			campaignID = cid
			eventType = unsafeString(pbReq.EventType)
			if pbReq.Metadata != nil {
				userID = unsafeString(pbReq.Metadata.UserId)
				if len(pbReq.Metadata.ClickId) > 0 {
					clickID = unsafeString(pbReq.Metadata.ClickId)
				}
				if len(pbReq.Metadata.ExtraBytes) > 0 {
					payload = pbReq.Metadata.ExtraBytes
				} else if pbReq.Metadata.Extra != nil {
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

			req := trackRequestPool.Get().(*TrackRequest)
			req.CampaignID = uuid.Nil
			req.UserID = ""
			req.Type = ""
			req.ClickID = ""
			req.Payload = req.Payload[:0]
			defer trackRequestPool.Put(req)

			if err := req.UnmarshalJSON(buf.Bytes()); err != nil {
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

		if clickID == "" {
			requestIDStr = unsafeString(wReqID.buf)
			clickID = requestIDStr
		}

		evt := domain.EventPool.Get().(*domain.Event)
		evt.Reset()
		evt.ClickID = clickID
		evt.CampaignID = campaignID
		evt.UserID = userID
		evt.Type = eventType
		evt.Payload = append(evt.Payload[:0], payload...)
		evt.IP = ip
		ua := ""
		if uaSlice := r.Header["User-Agent"]; len(uaSlice) > 0 {
			ua = uaSlice[0]
		}
		evt.UA = ua

		if filterEngine != nil {
			filterCtx, filterCancel := context.WithTimeout(r.Context(), time.Duration(cfg.FilterTimeoutMs)*time.Millisecond)
			defer filterCancel()

			if err := filterEngine.Check(filterCtx, evt); err != nil {
				if errors.Is(err, ErrEmergencyBreakerActive) {
					domain.EventPool.Put(evt)
					slog.Warn("event rejected: emergency breaker active", "request_id", id)
					metrics.FilterBlockedTotal.WithLabelValues("emergency_breaker").Inc()
					http.Error(w, "service temporarily unavailable", http.StatusServiceUnavailable)
					return
				} else if errors.Is(err, ErrRateLimitExceeded) {
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
				} else if errors.Is(err, ErrCampaignNotFound) {
					domain.EventPool.Put(evt)
					slog.Warn("event rejected: campaign not found", "request_id", id)
					metrics.FilterBlockedTotal.WithLabelValues("campaign_not_found").Inc()
					http.Error(w, "campaign not found", http.StatusNotFound)
					return
				} else if errors.Is(err, ErrBidFloorNotMet) {
					domain.EventPool.Put(evt)
					slog.Warn("event rejected: bid floor not met", "request_id", id)
					metrics.FilterBlockedTotal.WithLabelValues("bid_floor").Inc()
					http.Error(w, "bid floor not met", http.StatusPaymentRequired)
					return
				} else if errors.Is(err, context.DeadlineExceeded) {
					domain.EventPool.Put(evt)
					slog.Warn("event rejected: filter timeout", "request_id", id)
					metrics.FilterBlockedTotal.WithLabelValues("filter_timeout").Inc()
					http.Error(w, "filter timeout", http.StatusGatewayTimeout)
					return
				} else if errors.Is(err, ErrFraudDetected) {
					slog.Warn("fraud detected: silent drop", "reason", evt.FraudReason, "request_id", id)
					metrics.FilterBlockedTotal.WithLabelValues("fraud").Inc()

					shard := sharder.GetShard(evt.CampaignID)
					rdb := rdbs[shard]

					valSlicePtr := fraudValuesPool.Get().(*[]any)
					valSlice := *valSlicePtr

					wCamp := bufPool.Get().(*bufWrapper)
					wCamp.buf = wCamp.buf[:0]
					wCamp.buf = appendUUID(wCamp.buf, evt.CampaignID)
					campIDStr := unsafeString(wCamp.buf)

					wTime := bufPool.Get().(*bufWrapper)
					wTime.buf = wTime.buf[:0]
					wTime.buf = evt.CreatedAt.AppendFormat(wTime.buf, time.RFC3339Nano)
					timeStr := unsafeString(wTime.buf)

					payloadStr := unsafeString(evt.Payload)

					valSlice[0] = "click_id"
					valSlice[1] = evt.ClickID
					valSlice[2] = "campaign_id"
					valSlice[3] = campIDStr
					valSlice[4] = "user_id"
					valSlice[5] = evt.UserID
					valSlice[6] = "type"
					valSlice[7] = evt.Type
					valSlice[8] = "ip"
					valSlice[9] = evt.IP
					valSlice[10] = "ua"
					valSlice[11] = evt.UA
					valSlice[12] = "payload"
					valSlice[13] = payloadStr
					valSlice[14] = "fraud_reason"
					valSlice[15] = evt.FraudReason
					valSlice[16] = "created_at"
					valSlice[17] = timeStr

					rdbErr := rdb.XAdd(context.WithoutCancel(r.Context()), &redis.XAddArgs{
						Stream: fraudStream,
						MaxLen: int64(cfg.StreamMaxLen),
						Approx: true,
						Values: valSlice,
					}).Err()

					fraudValuesPool.Put(valSlicePtr)

					bufPool.Put(wCamp)
					bufPool.Put(wTime)

					if rdbErr != nil {
						slog.Error("failed to write to fraud stream", "error", rdbErr, "request_id", id)
					}

					domain.EventPool.Put(evt)
					accept := ""
					if accSlice := r.Header["Accept"]; len(accSlice) > 0 {
						accept = accSlice[0]
					}
					writeHTTPTrackAccepted(w, wReqID, requestIDStr, accept)
					return

				} else {
					domain.EventPool.Put(evt)
					slog.Error("filter engine failure", "error", err, "request_id", id)
					http.Error(w, "internal error", http.StatusInternalServerError)
					return
				}
			}
		}

		domain.EventPool.Put(evt)

		accept := ""
		if accSlice := r.Header["Accept"]; len(accSlice) > 0 {
			accept = accSlice[0]
		}
		writeHTTPTrackAccepted(w, wReqID, requestIDStr, accept)
	})

	return mux
}

func isTrustedProxy(ipStr string, trustedProxies []string) bool {
	if len(trustedProxies) == 0 {
		return false
	}
	parsedIP := net.ParseIP(ipStr)
	if parsedIP == nil {
		return false
	}
	for _, p := range trustedProxies {
		if p == "" {
			continue
		}
		if p == ipStr {
			return true
		}
		if _, ipNet, err := net.ParseCIDR(p); err == nil {
			if ipNet.Contains(parsedIP) {
				return true
			}
		}
	}
	return false
}

func getIPOnly(addr string) string {
	if idx := strings.LastIndexByte(addr, ':'); idx != -1 {
		if idx > 0 && addr[idx-1] == ']' {
			if addr[0] == '[' {
				return addr[1 : idx-1]
			}
		}
		return addr[:idx]
	}
	return addr
}

func extractClientIP(r *http.Request, trustedProxies []string) string {
	remoteIP := getIPOnly(r.RemoteAddr)
	if !isTrustedProxy(remoteIP, trustedProxies) {
		return remoteIP
	}

	var xff string
	if xffSlice := r.Header["X-Forwarded-For"]; len(xffSlice) > 0 {
		xff = xffSlice[0]
	}
	if xff != "" {
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

	if xriSlice := r.Header["X-Real-Ip"]; len(xriSlice) > 0 {
		xri := xriSlice[0]
		ipStr := strings.TrimSpace(xri)
		parsedIP := net.ParseIP(ipStr)
		if parsedIP != nil && !parsedIP.IsPrivate() && !parsedIP.IsLoopback() && !parsedIP.IsLinkLocalUnicast() {
			return ipStr
		}
	}

	return remoteIP
}

var (
	respHealth            = []byte("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nOK")
	respHealthUnavailable = []byte("HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\nContent-Length: 19\r\nConnection: keep-alive\r\n\r\nservice unavailable")
	respMetricsError      = []byte("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
	respInvalidProto      = []byte("HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: 16\r\nConnection: keep-alive\r\n\r\ninvalid protobuf")
	respInvalidCampaign   = []byte("HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: 19\r\nConnection: keep-alive\r\n\r\ninvalid campaign_id")
	respInvalidJSON       = []byte("HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: 12\r\nConnection: keep-alive\r\n\r\ninvalid json")
	respEmergencyBreaker  = []byte("HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\nContent-Length: 32\r\nConnection: keep-alive\r\n\r\nservice temporarily unavailable")
	respRateLimit         = []byte("HTTP/1.1 429 Too Many Requests\r\nContent-Type: text/plain\r\nContent-Length: 19\r\nConnection: keep-alive\r\n\r\nrate limit exceeded")
	respDuplicate         = []byte("HTTP/1.1 409 Conflict\r\nContent-Type: text/plain\r\nContent-Length: 15\r\nConnection: keep-alive\r\n\r\nduplicate event")
	respBudget            = []byte("HTTP/1.1 402 Payment Required\r\nContent-Type: text/plain\r\nContent-Length: 16\r\nConnection: keep-alive\r\n\r\nbudget exhausted")
	respPacing            = []byte("HTTP/1.1 429 Too Many Requests\r\nContent-Type: text/plain\r\nContent-Length: 20\r\nConnection: keep-alive\r\n\r\npacing limit reached")
	respFreq              = []byte("HTTP/1.1 403 Forbidden\r\nContent-Type: text/plain\r\nContent-Length: 23\r\nConnection: keep-alive\r\n\r\nfrequency limit reached")
	respGeo               = []byte("HTTP/1.1 403 Forbidden\r\nContent-Type: text/plain\r\nContent-Length: 21\r\nConnection: keep-alive\r\n\r\ngeo-targeting blocked")
	respCampaignNotFound  = []byte("HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: 17\r\nConnection: keep-alive\r\n\r\ncampaign not found")
	respBidFloorNotMet    = []byte("HTTP/1.1 402 Payment Required\r\nContent-Type: text/plain\r\nContent-Length: 17\r\nConnection: keep-alive\r\n\r\nbid floor not met")
	respFilterTimeout     = []byte("HTTP/1.1 504 Gateway Timeout\r\nContent-Type: text/plain\r\nContent-Length: 15\r\nConnection: keep-alive\r\n\r\nfilter timeout")
	respInternalError     = []byte("HTTP/1.1 500 Internal Server Error\r\nContent-Type: text/plain\r\nContent-Length: 14\r\nConnection: keep-alive\r\n\r\ninternal error")
	respBadRequestClose   = []byte("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
	respNotFound          = []byte("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
	respMethodNotAllowed  = []byte("HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
	respPayloadTooLarge   = []byte("HTTP/1.1 413 Payload Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
)

// AdsPacketHandler is the production gnet event engine handler. It implements
// gnet.EventHandler to process raw TCP data without kernel-space copies or Go's
// http.Server scheduling overhead. Each method executes on a fixed set of OS
// threads managed by gnet's event loops; no goroutine is spawned per connection.
// The 600-element trackStatusCounters array is indexed directly by HTTP status code,
// removing the prometheus label-set lookup from the hot path.
type AdsPacketHandler struct {
	*gnet.BuiltinEventEngine
	eng                   *gnet.Engine
	filterEngine          *FilterEngine
	cfg                   *config.Config
	pool                  Pinger
	rdbs                  []redis.UniversalClient
	sharder               Sharder
	fraudStream           string
	trackDurationObserver prometheus.Observer
	trackStatusCounters   [600]prometheus.Counter
	healthy               atomic.Int32
	logger                *logger.Logger
	loggerShardCounter    atomic.Uint64
}

func (h *AdsPacketHandler) SetLogger(l *logger.Logger) {
	h.logger = l
}

// NewAdsPacketHandler pre-allocates all Prometheus observer and counter objects
// before the server starts accepting traffic, ensuring the WithLabelValues hot path
// is never hit during request processing. The returned handler must be passed to
// gnet.Run; it is not safe to call React directly without an active gnet engine.
func NewAdsPacketHandler(cfg *config.Config, registry domain.CampaignRegistry, filterEngine *FilterEngine, pool Pinger, rdbs []redis.UniversalClient, sharder Sharder, fraudStream string) *AdsPacketHandler {
	trackDurationObserver := metrics.HttpRequestDuration.WithLabelValues("POST", "/track")
	var trackStatusCounters [600]prometheus.Counter
	for i := 0; i < 600; i++ {
		trackStatusCounters[i] = metrics.HttpRequestsTotal.WithLabelValues("POST", "/track", statusStrings[i])
	}

	return &AdsPacketHandler{
		filterEngine:          filterEngine,
		cfg:                   cfg,
		pool:                  pool,
		rdbs:                  rdbs,
		sharder:               sharder,
		fraudStream:           fraudStream,
		trackDurationObserver: trackDurationObserver,
		trackStatusCounters:   trackStatusCounters,
	}
}

func (h *AdsPacketHandler) recordMetrics(start time.Time, status int) {
	duration := time.Since(start).Seconds()
	if status >= 0 && status < 600 {
		h.trackStatusCounters[status].Inc()
	} else {
		metrics.HttpRequestsTotal.WithLabelValues("POST", "/track", strconv.Itoa(status)).Inc()
	}
	h.trackDurationObserver.Observe(duration)
}

func (h *AdsPacketHandler) writeGnetTrackAccepted(ctx *connContext, req parsedHTTPRequest, c gnet.Conn, start time.Time, wReqID *bufWrapper, requestIDStr string) {
	if requestIDStr == "" {
		requestIDStr = unsafeString(wReqID.buf)
	}

	accept := unsafeString(req.Accept)
	if accept == "application/x-protobuf" {
		resp := &ctx.resp
		resp.Reset()
		resp.RequestId = requestIDStr
		resp.Status = "accepted"

		respSize := resp.SizeVT()
		bufSlice := ctx.bufSlice
		if cap(bufSlice) < 200+respSize {
			bufSlice = make([]byte, 200+respSize)
			ctx.bufSlice = bufSlice
		} else {
			bufSlice = bufSlice[:200+respSize]
		}

		offset := copy(bufSlice, "HTTP/1.1 202 Accepted\r\nContent-Type: application/x-protobuf\r\nContent-Length: ")
		offset += copy(bufSlice[offset:], strconv.Itoa(respSize))
		offset += copy(bufSlice[offset:], "\r\nConnection: keep-alive\r\n\r\n")

		n, err := resp.MarshalToVT(bufSlice[offset : offset+respSize])
		if err != nil {
			c.Write(respInternalError)
			h.recordMetrics(start, http.StatusInternalServerError)
			return
		}
		outSlice := bufSlice[:offset+n]
		metrics.GnetBytesSent.Add(float64(len(outSlice)))
		metrics.GnetPacketsSent.Inc()
		c.Write(outSlice)
	} else {
		respSize := len(`{"request_id":"","status":"accepted"}`) + len(requestIDStr)
		bufSlice := ctx.bufSlice
		if cap(bufSlice) < 200+respSize {
			bufSlice = make([]byte, 200+respSize)
			ctx.bufSlice = bufSlice
		} else {
			bufSlice = bufSlice[:200+respSize]
		}

		offset := copy(bufSlice, "HTTP/1.1 202 Accepted\r\nContent-Type: application/json\r\nContent-Length: ")
		offset += copy(bufSlice[offset:], strconv.Itoa(respSize))
		offset += copy(bufSlice[offset:], "\r\nConnection: keep-alive\r\n\r\n")
		offset += copy(bufSlice[offset:], `{"request_id":"`)
		offset += copy(bufSlice[offset:], requestIDStr)
		offset += copy(bufSlice[offset:], `","status":"accepted"}`)

		metrics.GnetBytesSent.Add(float64(offset))
		metrics.GnetPacketsSent.Inc()
		c.Write(bufSlice[:offset])
	}

	h.recordMetrics(start, http.StatusAccepted)
}

func writeHTTPTrackAccepted(w http.ResponseWriter, wReqID *bufWrapper, requestIDStr string, accept string) {
	if requestIDStr == "" {
		requestIDStr = unsafeString(wReqID.buf)
	}
	if accept == "application/x-protobuf" {
		resp := trackResponsePool.Get().(*pb.TrackResponse)
		defer putTrackResponse(resp)
		resp.RequestId = requestIDStr
		resp.Status = "accepted"

		respSize := resp.SizeVT()
		bufSlicePtr := responseBytesPool.Get().(*[]byte)
		bufSlice := *bufSlicePtr
		if cap(bufSlice) < respSize {
			bufSlice = make([]byte, respSize)
		} else {
			bufSlice = bufSlice[:respSize]
		}

		n, err := resp.MarshalToVT(bufSlice)
		if err != nil {
			*bufSlicePtr = bufSlice
			responseBytesPool.Put(bufSlicePtr)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		out := bufSlice[:n]
		w.Header()["Content-Type"] = contentTypeProtoHeader
		w.WriteHeader(http.StatusAccepted)
		w.Write(out)
		*bufSlicePtr = bufSlice
		responseBytesPool.Put(bufSlicePtr)
		return
	}

	w.Header()["Content-Type"] = contentTypeJsonHeader
	w.WriteHeader(http.StatusAccepted)
	buf := bufferPool.Get().(*bytes.Buffer)
	defer putBuffer(buf)
	buf.WriteString(`{"request_id":"`)
	buf.Write(wReqID.buf)
	buf.WriteString(`","status":"accepted"}`)
	w.Write(buf.Bytes())
}

func (h *AdsPacketHandler) OnBoot(eng gnet.Engine) (action gnet.Action) {
	slog.Info("gnet server is booting")
	h.eng = &eng
	return gnet.None
}

func (h *AdsPacketHandler) Stop(ctx context.Context) error {
	if h.eng != nil {
		return h.eng.Stop(ctx)
	}
	return nil
}

func (h *AdsPacketHandler) StartHealthProbe(ctx context.Context) {
	h.healthy.Store(1)
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				ok := true
				if h.pool != nil {
					if err := h.pool.Ping(probeCtx); err != nil {
						ok = false
						slog.Error("health probe: postgres unreachable", "error", err)
					}
				}
				for i, rdb := range h.rdbs {
					if err := rdb.Ping(probeCtx).Err(); err != nil {
						ok = false
						slog.Error("health probe: redis shard unreachable", "shard", i, "error", err)
					}
				}
				cancel()
				if ok {
					h.healthy.Store(1)
				} else {
					h.healthy.Store(0)
				}
			}
		}
	}()
}

func (h *AdsPacketHandler) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	metrics.GnetActiveConnections.Inc()
	return nil, gnet.None
}

func (h *AdsPacketHandler) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	metrics.GnetActiveConnections.Dec()
	return gnet.None
}

// OnTraffic is the gnet reactor callback invoked whenever new data arrives on a
// connection. It loops over all complete HTTP/1.1 requests present in the inbound
// ring buffer using Peek (zero-copy read) and Discard (advance read pointer), then
// delegates to React for each parsed request. Partial requests (errIncompleteRequest)
// break the loop and wait for the next OnTraffic call once the OS delivers the
// remaining bytes.
func (h *AdsPacketHandler) OnTraffic(c gnet.Conn) (action gnet.Action) {

	loopStart := time.Now()
	defer func() {
		metrics.GnetEventLoopWorkDuration.Add(time.Since(loopStart).Seconds())
	}()

	for {
		inboundBuffered := c.InboundBuffered()
		if inboundBuffered == 0 {
			break
		}
		buf, err := c.Peek(inboundBuffered)
		if err != nil {
			return gnet.Close
		}

		metrics.GnetBytesReceived.Add(float64(len(buf)))
		metrics.GnetPacketsReceived.Inc()

		reqLen, req, err := h.parseHTTP(buf)
		if err != nil {
			if errors.Is(err, errIncompleteRequest) {
				metrics.HttpParseErrors.WithLabelValues("incomplete").Inc()
				break
			}
			if errors.Is(err, errPayloadTooLarge) {
				metrics.HttpParseErrors.WithLabelValues("payload_too_large").Inc()
				c.Write(respPayloadTooLarge)
				return gnet.Close
			}
			metrics.HttpParseErrors.WithLabelValues("invalid").Inc()
			c.Write(respBadRequestClose)
			return gnet.Close
		}

		act := h.React(req, c)
		if _, err := c.Discard(reqLen); err != nil {
			return gnet.Close
		}

		if act != gnet.None {
			return act
		}
	}
	return gnet.None
}

// parsedHTTPRequest holds byte-slice references into the gnet inbound ring buffer.
// All fields are slices of the original buffer - no copies are made during header
// parsing. Callers must not retain these slices past the scope of a single React call
// because Discard will advance the ring-buffer read pointer, invalidating the memory.
type parsedHTTPRequest struct {
	Method           []byte
	Path             []byte
	ContentType      []byte
	ClientIP         []byte
	UserAgent        []byte
	Accept           []byte
	Body             []byte
	ContentLength    int
	HasContentLength bool
}

var (
	errIncompleteRequest = errors.New("incomplete HTTP request")
	errInvalidRequest    = errors.New("invalid HTTP request")
	errPayloadTooLarge   = errors.New("payload too large")
)

func (h *AdsPacketHandler) parseHTTP(data []byte) (int, parsedHTTPRequest, error) {
	var req parsedHTTPRequest

	lineEnd := bytes.Index(data, []byte("\r\n"))
	if lineEnd < 0 {
		return 0, req, errIncompleteRequest
	}
	reqLine := data[:lineEnd]

	space1 := bytes.IndexByte(reqLine, ' ')
	if space1 < 0 {
		return 0, req, errInvalidRequest
	}
	req.Method = reqLine[:space1]

	rest := reqLine[space1+1:]
	space2 := bytes.IndexByte(rest, ' ')
	if space2 < 0 {
		return 0, req, errInvalidRequest
	}
	req.Path = rest[:space2]

	idx := lineEnd + 2
	for {
		if idx >= len(data) {
			return 0, req, errIncompleteRequest
		}
		if idx+2 <= len(data) && data[idx] == '\r' && data[idx+1] == '\n' {
			idx += 2
			break
		}

		lineEnd = bytes.Index(data[idx:], []byte("\r\n"))
		if lineEnd < 0 {
			return 0, req, errIncompleteRequest
		}
		headerLine := data[idx : idx+lineEnd]
		idx += lineEnd + 2

		colonIdx := bytes.IndexByte(headerLine, ':')
		if colonIdx < 0 {
			continue
		}

		key := trimSpaceBytes(headerLine[:colonIdx])
		val := trimSpaceBytes(headerLine[colonIdx+1:])

		if equalFoldBytes(key, []byte("content-length")) {
			req.ContentLength = parseDecimal(val)
			req.HasContentLength = true
		} else if equalFoldBytes(key, []byte("content-type")) {
			req.ContentType = val
		} else if equalFoldBytes(key, []byte("x-forwarded-for")) {
			req.ClientIP = val
		} else if equalFoldBytes(key, []byte("x-real-ip")) {
			if len(req.ClientIP) == 0 {
				req.ClientIP = val
			}
		} else if equalFoldBytes(key, []byte("user-agent")) {
			req.UserAgent = val
		} else if equalFoldBytes(key, []byte("accept")) {
			req.Accept = val
		}
	}

	if req.HasContentLength && int64(req.ContentLength) > h.cfg.MaxRequestBodySize {
		return 0, req, errPayloadTooLarge
	}

	totalLen := idx + req.ContentLength
	if len(data) < totalLen {
		return 0, req, errIncompleteRequest
	}
	req.Body = data[idx : idx+req.ContentLength]
	return totalLen, req, nil
}

func (h *AdsPacketHandler) React(req parsedHTTPRequest, c gnet.Conn) gnet.Action {
	ctx, ok := c.Context().(*connContext)
	if !ok {
		ctx = &connContext{
			pbReq: pb.AdEvent{
				Metadata: &pb.EventMetadata{},
			},
			reqJSON: TrackRequest{
				Payload: make([]byte, 0, 512),
			},
			evt: domain.Event{
				Payload: make([]byte, 0, 1024),
			},
			valSlice: make([]any, 18),
			resp:     pb.TrackResponse{},
			bufSlice: make([]byte, 4096),
			wReqID: bufWrapper{
				buf: make([]byte, 0, 128),
			},
			wCamp: bufWrapper{
				buf: make([]byte, 0, 128),
			},
			wTime: bufWrapper{
				buf: make([]byte, 0, 128),
			},
		}
		if h.logger != nil {
			ctx.shardID = int(h.loggerShardCounter.Add(1) % uint64(len(h.logger.Shards())))
		}
		c.SetContext(ctx)
	}

	if len(req.Method) == 3 && req.Method[0] == 'G' && req.Method[1] == 'E' && req.Method[2] == 'T' {
		if bytes.Equal(req.Path, []byte("/health")) {
			if h.healthy.Load() == 1 {
				c.Write(respHealth)
			} else {
				c.Write(respHealthUnavailable)
			}
			return gnet.None
		}
		if bytes.Equal(req.Path, []byte("/metrics")) {
			mfs, err := prometheus.DefaultGatherer.Gather()
			if err != nil {
				c.Write(respMetricsError)
				return gnet.None
			}

			bufMetrics := &ctx.bufMetrics
			bufMetrics.Reset()

			for _, mf := range mfs {
				_, _ = expfmt.MetricFamilyToText(bufMetrics, mf)
			}

			respSize := 200 + bufMetrics.Len()
			bufSlice := ctx.bufSlice
			if cap(bufSlice) < respSize {
				bufSlice = make([]byte, respSize)
				ctx.bufSlice = bufSlice
			} else {
				bufSlice = bufSlice[:respSize]
			}

			offset := copy(bufSlice, "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: ")
			offset += copy(bufSlice[offset:], strconv.Itoa(bufMetrics.Len()))
			offset += copy(bufSlice[offset:], "\r\nConnection: keep-alive\r\n\r\n")
			offset += copy(bufSlice[offset:], bufMetrics.Bytes())

			c.Write(bufSlice[:offset])
			return gnet.None
		}
		c.Write(respMethodNotAllowed)
		return gnet.None
	}

	isPOST := len(req.Method) == 4 && req.Method[0] == 'P' && req.Method[1] == 'O' && req.Method[2] == 'S' && req.Method[3] == 'T'
	if !isPOST {
		c.Write(respMethodNotAllowed)
		return gnet.None
	}

	if !bytes.Equal(req.Path, []byte("/track")) {
		c.Write(respNotFound)
		return gnet.None
	}

	if !req.HasContentLength {
		c.Write(respBadRequestClose)
		return gnet.Close
	}

	start := time.Now()
	var status int

	ip := extractClientIPGnet(ctx, &req, c, h.cfg.TrustedProxies)
	ua := unsafeString(req.UserAgent)

	var campaignID uuid.UUID
	var eventType string
	var userID string
	var payload []byte
	var clickID string
	var requestIDStr string

	uuidStart := time.Now()
	id, _ := NewFastUUID()

	metrics.UuidGenDuration.Observe(float64(time.Since(uuidStart).Nanoseconds()))
	wReqID := &ctx.wReqID
	wReqID.buf = wReqID.buf[:0]
	wReqID.buf = appendUUID(wReqID.buf, id)

	contentType := unsafeString(req.ContentType)
	if contentType == "application/x-protobuf" || contentType == "" {
		metrics.FilterThroughput.WithLabelValues("protobuf").Inc()
		pbReq := &ctx.pbReq
		pbReq.CampaignId = pbReq.CampaignId[:0]
		pbReq.EventType = pbReq.EventType[:0]
		if pbReq.Metadata != nil {
			pbReq.Metadata.ClickId = pbReq.Metadata.ClickId[:0]
			pbReq.Metadata.UserId = pbReq.Metadata.UserId[:0]
			pbReq.Metadata.DeviceType = pbReq.Metadata.DeviceType[:0]
			pbReq.Metadata.Os = pbReq.Metadata.Os[:0]
			for k := range pbReq.Metadata.Extra {
				delete(pbReq.Metadata.Extra, k)
			}
			pbReq.Metadata.ExtraBytes = pbReq.Metadata.ExtraBytes[:0]
		}

		if err := pbReq.UnmarshalVT(req.Body); err != nil {
			status = http.StatusBadRequest
			c.Write(respInvalidProto)
			h.recordMetrics(start, status)
			return gnet.None
		}

		if len(pbReq.CampaignId) != 16 {
			status = http.StatusBadRequest
			c.Write(respInvalidCampaign)
			h.recordMetrics(start, status)
			return gnet.None
		}
		copy(campaignID[:], pbReq.CampaignId)

		eventType = unsafeString(pbReq.EventType)
		if pbReq.Metadata != nil {
			userID = unsafeString(pbReq.Metadata.UserId)
			if len(pbReq.Metadata.ClickId) > 0 {
				clickID = unsafeString(pbReq.Metadata.ClickId)
			}
			if len(pbReq.Metadata.ExtraBytes) > 0 {
				payload = pbReq.Metadata.ExtraBytes
			} else if pbReq.Metadata.Extra != nil {
				payload, _ = json.Marshal(pbReq.Metadata.Extra)
			}
		}
	} else {
		metrics.FilterThroughput.WithLabelValues("json").Inc()
		reqJSON := &ctx.reqJSON
		reqJSON.CampaignID = uuid.Nil
		reqJSON.UserID = ""
		reqJSON.Type = ""
		reqJSON.ClickID = ""
		reqJSON.Payload = reqJSON.Payload[:0]

		if err := reqJSON.UnmarshalJSON(req.Body); err != nil {
			status = http.StatusBadRequest
			c.Write(respInvalidJSON)
			h.recordMetrics(start, status)
			return gnet.None
		}
		campaignID = reqJSON.CampaignID
		userID = reqJSON.UserID
		eventType = reqJSON.Type
		payload = reqJSON.Payload
		if reqJSON.ClickID != "" {
			clickID = reqJSON.ClickID
		}
	}

	if clickID == "" {
		requestIDStr = unsafeString(wReqID.buf)
		clickID = requestIDStr
	}

	evt := &ctx.evt
	evt.Reset()
	evt.ClickID = clickID
	evt.CampaignID = campaignID
	evt.UserID = userID
	evt.Type = eventType
	evt.Payload = append(evt.Payload[:0], payload...)
	evt.IP = ip
	evt.UA = ua

	if h.filterEngine != nil {
		filterCtx, filterCancel := context.WithTimeout(context.Background(), time.Duration(h.cfg.FilterTimeoutMs)*time.Millisecond)
		defer filterCancel()

		if err := h.filterEngine.Check(filterCtx, evt); err != nil {
			if errors.Is(err, ErrEmergencyBreakerActive) {
				metrics.FilterBlockedTotal.WithLabelValues("emergency_breaker").Inc()
				metrics.FilterDecisions.WithLabelValues("emergency_breaker").Inc()
				status = http.StatusServiceUnavailable
				c.Write(respEmergencyBreaker)
				h.recordMetrics(start, status)
				return gnet.None
			} else if errors.Is(err, ErrRateLimitExceeded) {
				metrics.FilterBlockedTotal.WithLabelValues("rate_limit").Inc()
				metrics.FilterDecisions.WithLabelValues("rate_limited").Inc()
				status = http.StatusTooManyRequests
				c.Write(respRateLimit)
				h.recordMetrics(start, status)
				return gnet.None
			} else if errors.Is(err, ErrDuplicateEvent) {
				metrics.FilterBlockedTotal.WithLabelValues("duplicate").Inc()
				metrics.FilterDecisions.WithLabelValues("duplicate").Inc()
				status = http.StatusConflict
				c.Write(respDuplicate)
				h.recordMetrics(start, status)
				return gnet.None
			} else if errors.Is(err, ErrBudgetExhausted) {
				metrics.FilterBlockedTotal.WithLabelValues("budget").Inc()
				metrics.FilterDecisions.WithLabelValues("budget_exhausted").Inc()
				status = http.StatusPaymentRequired
				c.Write(respBudget)
				h.recordMetrics(start, status)
				return gnet.None
			} else if errors.Is(err, ErrPacingExhausted) {
				metrics.FilterBlockedTotal.WithLabelValues("pacing").Inc()
				metrics.FilterDecisions.WithLabelValues("pacing_limit").Inc()
				status = http.StatusTooManyRequests
				c.Write(respPacing)
				h.recordMetrics(start, status)
				return gnet.None
			} else if errors.Is(err, ErrFreqLimitExceeded) {
				metrics.FilterBlockedTotal.WithLabelValues("freq").Inc()
				metrics.FilterDecisions.WithLabelValues("frequency_capped").Inc()
				status = http.StatusForbidden
				c.Write(respFreq)
				h.recordMetrics(start, status)
				return gnet.None
			} else if errors.Is(err, ErrGeoBlocked) {
				metrics.FilterBlockedTotal.WithLabelValues("geo").Inc()
				metrics.FilterDecisions.WithLabelValues("geo_blocked").Inc()
				status = http.StatusForbidden
				c.Write(respGeo)
				h.recordMetrics(start, status)
				return gnet.None
			} else if errors.Is(err, ErrCampaignNotFound) {
				metrics.FilterBlockedTotal.WithLabelValues("campaign_not_found").Inc()
				metrics.FilterDecisions.WithLabelValues("campaign_not_found").Inc()
				status = http.StatusNotFound
				c.Write(respCampaignNotFound)
				h.recordMetrics(start, status)
				return gnet.None
			} else if errors.Is(err, ErrBidFloorNotMet) {
				metrics.FilterBlockedTotal.WithLabelValues("bid_floor").Inc()
				metrics.FilterDecisions.WithLabelValues("bid_floor").Inc()
				status = http.StatusPaymentRequired
				c.Write(respBidFloorNotMet)
				h.recordMetrics(start, status)
				return gnet.None
			} else if errors.Is(err, context.DeadlineExceeded) {
				metrics.FilterBlockedTotal.WithLabelValues("filter_timeout").Inc()
				metrics.FilterDecisions.WithLabelValues("filter_timeout").Inc()
				status = http.StatusGatewayTimeout
				c.Write(respFilterTimeout)
				h.recordMetrics(start, status)
				return gnet.None
			} else if errors.Is(err, ErrFraudDetected) {
				metrics.FilterBlockedTotal.WithLabelValues("fraud").Inc()
				metrics.FilterDecisions.WithLabelValues("fraud").Inc()

				shard := h.sharder.GetShard(evt.CampaignID)
				rdb := h.rdbs[shard]

				valSlice := ctx.valSlice

				wCamp := &ctx.wCamp
				wCamp.buf = wCamp.buf[:0]
				wCamp.buf = appendUUID(wCamp.buf, evt.CampaignID)
				campIDStr := unsafeString(wCamp.buf)

				wTime := &ctx.wTime
				wTime.buf = wTime.buf[:0]
				wTime.buf = evt.CreatedAt.AppendFormat(wTime.buf, time.RFC3339Nano)
				timeStr := unsafeString(wTime.buf)

				payloadStr := unsafeString(evt.Payload)

				valSlice[0] = "click_id"
				valSlice[1] = evt.ClickID
				valSlice[2] = "campaign_id"
				valSlice[3] = campIDStr
				valSlice[4] = "user_id"
				valSlice[5] = evt.UserID
				valSlice[6] = "type"
				valSlice[7] = evt.Type
				valSlice[8] = "ip"
				valSlice[9] = evt.IP
				valSlice[10] = "ua"
				valSlice[11] = evt.UA
				valSlice[12] = "payload"
				valSlice[13] = payloadStr
				valSlice[14] = "fraud_reason"
				valSlice[15] = evt.FraudReason
				valSlice[16] = "created_at"
				valSlice[17] = timeStr

				_ = rdb.XAdd(context.Background(), &redis.XAddArgs{
					Stream: h.fraudStream,
					MaxLen: int64(h.cfg.StreamMaxLen),
					Approx: true,
					Values: valSlice,
				}).Err()

				h.writeGnetTrackAccepted(ctx, req, c, start, wReqID, requestIDStr)
				return gnet.None
			} else {
				status = http.StatusInternalServerError
				c.Write(respInternalError)
				h.recordMetrics(start, status)
				return gnet.None
			}
		}
	}

	metrics.FilterDecisions.WithLabelValues("accepted").Inc()
	if h.logger != nil {
		rec := adLogRecordPool.Get().(*pb.AdLogRecord)
		rec.TimestampUnix = start.Unix()
		if cap(rec.CampaignId) < 16 {
			rec.CampaignId = make([]byte, 16)
		} else {
			rec.CampaignId = rec.CampaignId[:16]
		}
		copy(rec.CampaignId, campaignID[:])
		rec.ClickId = UnsafeBytes(clickID)
		rec.EventType = UnsafeBytes(eventType)
		rec.Priority = 0

		size := rec.SizeVT()
		bufPtr := logBufPool.Get().(*[]byte)
		buf := *bufPtr
		if cap(buf) < size {
			buf = make([]byte, size)
		} else {
			buf = buf[:size]
		}

		n, err := rec.MarshalToSizedBufferVT(buf)
		if err == nil {
			h.logger.WriteToShard(ctx.shardID, 0, buf[:n])
		}
		*bufPtr = buf
		logBufPool.Put(bufPtr)

		campIDSaved := rec.CampaignId
		rec.Reset()
		if cap(campIDSaved) >= 16 {
			rec.CampaignId = campIDSaved[:0]
		}
		adLogRecordPool.Put(rec)
	}
	h.writeGnetTrackAccepted(ctx, req, c, start, wReqID, requestIDStr)
	return gnet.None
}

func extractClientIPGnet(ctx *connContext, req *parsedHTTPRequest, c gnet.Conn, trustedProxies []string) string {
	if ctx.remoteIP == "" {
		ctx.remoteIP = getIPOnly(c.RemoteAddr().String())
	}
	remoteIP := ctx.remoteIP
	if !isTrustedProxy(remoteIP, trustedProxies) {
		return remoteIP
	}

	if len(req.ClientIP) > 0 {
		xff := unsafeString(req.ClientIP)
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

	return remoteIP
}

func trimSpaceBytes(b []byte) []byte {
	start := 0
	for start < len(b) && (b[start] == ' ' || b[start] == '\t') {
		start++
	}
	end := len(b)
	for end > start && (b[end-1] == ' ' || b[end-1] == '\t' || b[end-1] == '\r') {
		end--
	}
	return b[start:end]
}

func equalFoldBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		c1 := a[i]
		c2 := b[i]
		if c1 >= 'A' && c1 <= 'Z' {
			c1 += 'a' - 'A'
		}
		if c2 >= 'A' && c2 <= 'Z' {
			c2 += 'a' - 'A'
		}
		if c1 != c2 {
			return false
		}
	}
	return true
}

func parseDecimal(b []byte) int {
	val := 0
	for _, c := range b {
		if c >= '0' && c <= '9' {
			val = val*10 + int(c-'0')
		}
	}
	return val
}
