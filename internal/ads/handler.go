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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	"github.com/mykhailov-ua/ad-event-processor/internal/metrics"
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
	maxPoolObjectSize      = 64 * 1024 // 64KB
	contentTypeProtoHeader = []string{"application/x-protobuf"}
	contentTypeJsonHeader  = []string{"application/json"}
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

type Pinger interface {
	Ping(ctx context.Context) error
}

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

		ip := extractClientIP(r)
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
			if err := filterEngine.Check(r.Context(), evt); err != nil {
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

					// Silent drop is enforced to prevent adversarial actors from discovering anti-fraud detection rules.
					// Fraud events are acknowledged with HTTP 202 status at the tracker layer while being logged asynchronously.
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
		if accept == "application/x-protobuf" {
			resp := trackResponsePool.Get().(*pb.TrackResponse)
			defer putTrackResponse(resp)

			if requestIDStr == "" {
				requestIDStr = unsafeString(wReqID.buf)
			}
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
				slog.Error("failed to marshal proto response", "error", err, "request_id", id)
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}
			out := bufSlice[:n]

			w.Header()["Content-Type"] = contentTypeProtoHeader
			w.WriteHeader(status)
			w.Write(out)

			*bufSlicePtr = bufSlice
			responseBytesPool.Put(bufSlicePtr)
		} else {
			w.Header()["Content-Type"] = contentTypeJsonHeader
			w.WriteHeader(status)

			buf := bufferPool.Get().(*bytes.Buffer)
			defer putBuffer(buf)

			buf.WriteString(`{"request_id":"`)
			buf.Write(wReqID.buf)
			buf.WriteString(`","status":"accepted"}`)
			w.Write(buf.Bytes())
		}
	})

	return mux
}

func extractClientIP(r *http.Request) string {
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

	addr := r.RemoteAddr
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

var (
	respHealth           = []byte("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nOK")
	respMetricsError     = []byte("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
	respInvalidProto     = []byte("HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: 16\r\nConnection: keep-alive\r\n\r\ninvalid protobuf")
	respInvalidCampaign  = []byte("HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: 19\r\nConnection: keep-alive\r\n\r\ninvalid campaign_id")
	respInvalidJSON      = []byte("HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: 12\r\nConnection: keep-alive\r\n\r\ninvalid json")
	respEmergencyBreaker = []byte("HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\nContent-Length: 32\r\nConnection: keep-alive\r\n\r\nservice temporarily unavailable")
	respRateLimit        = []byte("HTTP/1.1 429 Too Many Requests\r\nContent-Type: text/plain\r\nContent-Length: 19\r\nConnection: keep-alive\r\n\r\nrate limit exceeded")
	respDuplicate        = []byte("HTTP/1.1 409 Conflict\r\nContent-Type: text/plain\r\nContent-Length: 15\r\nConnection: keep-alive\r\n\r\nduplicate event")
	respBudget           = []byte("HTTP/1.1 402 Payment Required\r\nContent-Type: text/plain\r\nContent-Length: 16\r\nConnection: keep-alive\r\n\r\nbudget exhausted")
	respPacing           = []byte("HTTP/1.1 429 Too Many Requests\r\nContent-Type: text/plain\r\nContent-Length: 20\r\nConnection: keep-alive\r\n\r\npacing limit reached")
	respFreq             = []byte("HTTP/1.1 403 Forbidden\r\nContent-Type: text/plain\r\nContent-Length: 23\r\nConnection: keep-alive\r\n\r\nfrequency limit reached")
	respGeo              = []byte("HTTP/1.1 403 Forbidden\r\nContent-Type: text/plain\r\nContent-Length: 21\r\nConnection: keep-alive\r\n\r\ngeo-targeting blocked")
	respInternalError    = []byte("HTTP/1.1 500 Internal Server Error\r\nContent-Type: text/plain\r\nContent-Length: 14\r\nConnection: keep-alive\r\n\r\ninternal error")
	respBadRequestClose  = []byte("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
)

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
}

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

func (h *AdsPacketHandler) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	// Pin the OS thread to the current logical CPU so the event loop executes
	// entirely on one core, preserving L1/L2 cache affinity for connection state.
	runtime.LockOSThread()
	metrics.GnetActiveConnections.Inc()
	return nil, gnet.None
}

func (h *AdsPacketHandler) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	metrics.GnetActiveConnections.Dec()
	return gnet.None
}

func (h *AdsPacketHandler) OnTraffic(c gnet.Conn) (action gnet.Action) {
	// Record event loop saturation: wall-clock work time per reactor dispatch.
	// Ratio of this counter to elapsed real time gives event-loop utilization.
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

type parsedHTTPRequest struct {
	Method        []byte
	Path          []byte
	ContentType   []byte
	ClientIP      []byte
	UserAgent     []byte
	Accept        []byte
	Body          []byte
	ContentLength int
}

var (
	errIncompleteRequest = errors.New("incomplete HTTP request")
	errInvalidRequest    = errors.New("invalid HTTP request")
)

// parseHTTP parses basic HTTP/1.1 headers and payload directly from raw byte sequences.
//
// Memory Impact:
//   - Allocation-free. Parsed structure maps byte slices directly into the underlying gnet connection
//     ring buffer window to avoid slice allocation or header string copying.
//
// Concurrency:
// - Thread-safe. Executed sequentially within the active connection's event-loop thread.
//
// Performance Hacks:
//   - Ring Buffer Zero-Copy: Operates directly on the slice returned by gnet.Conn.Peek.
//   - Direct Scanner: Uses primitive byte scans (IndexByte, index loops) rather than regex or full-featured HTTP parsers
//     to optimize parser state machines.
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

	totalLen := idx + req.ContentLength
	if len(data) < totalLen {
		return 0, req, errIncompleteRequest
	}
	req.Body = data[idx : idx+req.ContentLength]
	return totalLen, req, nil
}

// React processes the parsed HTTP request, evaluates validation rules, executes geo and rate filtering,
// and serializes responses back into the gnet network stream.
//
// Memory Impact:
// - Near zero heap allocations. Recycles all high-frequency operational values via sync.Pool structures:
//   - adEventPool: Reuses pb.AdEvent objects.
//   - trackRequestPool: Reuses JSON parsing targets.
//   - responseBytesPool: Avoids heap-allocating output byte slices.
//   - bufPool & bufferPool: Recycling bytes.Buffer objects to prevent garbage collection pressure.
//
// Concurrency:
//   - Thread-safe. Designed for shared-nothing reactor threads. Executed exclusively inside connection-owned
//     event loops.
//
// Performance Hacks:
// - Bypasses standard reflect-based marshaling on outputs using vtproto fast path serialize methods.
// - Reuses connection memory directly during filter and serialization cycles.
func (h *AdsPacketHandler) React(req parsedHTTPRequest, c gnet.Conn) gnet.Action {
	if len(req.Method) == 3 && req.Method[0] == 'G' && req.Method[1] == 'E' && req.Method[2] == 'T' {
		if bytes.Equal(req.Path, []byte("/health")) {
			c.Write(respHealth)
			return gnet.None
		}
		if bytes.Equal(req.Path, []byte("/metrics")) {
			mfs, err := prometheus.DefaultGatherer.Gather()
			if err != nil {
				c.Write(respMetricsError)
				return gnet.None
			}

			bufMetrics := bufferPool.Get().(*bytes.Buffer)
			defer putBuffer(bufMetrics)

			for _, mf := range mfs {
				_, _ = expfmt.MetricFamilyToText(bufMetrics, mf)
			}

			respSize := 200 + bufMetrics.Len()
			bufSlicePtr := responseBytesPool.Get().(*[]byte)
			bufSlice := *bufSlicePtr
			if cap(bufSlice) < respSize {
				bufSlice = make([]byte, respSize)
			} else {
				bufSlice = bufSlice[:respSize]
			}

			offset := copy(bufSlice, "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: ")
			offset += copy(bufSlice[offset:], strconv.Itoa(bufMetrics.Len()))
			offset += copy(bufSlice[offset:], "\r\nConnection: keep-alive\r\n\r\n")
			offset += copy(bufSlice[offset:], bufMetrics.Bytes())

			c.Write(bufSlice[:offset])
			*bufSlicePtr = bufSlice
			responseBytesPool.Put(bufSlicePtr)
			return gnet.None
		}
	}

	start := time.Now()
	status := http.StatusAccepted

	defer func() {
		duration := time.Since(start).Seconds()
		if status >= 0 && status < 600 {
			h.trackStatusCounters[status].Inc()
		} else {
			metrics.HttpRequestsTotal.WithLabelValues("POST", "/track", strconv.Itoa(status)).Inc()
		}
		h.trackDurationObserver.Observe(duration)
	}()

	ip := extractClientIPGnet(&req, c.RemoteAddr())
	ua := unsafeString(req.UserAgent)

	var campaignID uuid.UUID
	var eventType string
	var userID string
	var payload []byte
	var clickID string
	var requestIDStr string

	uuidStart := time.Now()
	id, _ := NewFastUUID()
	// Observe NewFastUUID latency in nanoseconds so dashboard can detect
	// unexpected entropy source contention (e.g. getrandom syscall backpressure).
	metrics.UuidGenDuration.Observe(float64(time.Since(uuidStart).Nanoseconds()))
	wReqID := bufPool.Get().(*bufWrapper)
	wReqID.buf = wReqID.buf[:0]
	wReqID.buf = appendUUID(wReqID.buf, id)
	defer bufPool.Put(wReqID)

	contentType := unsafeString(req.ContentType)
	if contentType == "application/x-protobuf" || contentType == "" {
		metrics.FilterThroughput.WithLabelValues("protobuf").Inc()
		pbReq := adEventPool.Get().(*pb.AdEvent)
		defer putAdEvent(pbReq)

		if err := pbReq.UnmarshalVT(req.Body); err != nil {
			slog.Warn("invalid protobuf body", "error", err, "request_id", id)
			status = http.StatusBadRequest
			c.Write(respInvalidProto)
			return gnet.None
		}

		cid, err := uuid.ParseBytes(pbReq.CampaignId)
		if err != nil {
			slog.Warn("invalid campaign id in proto", "error", err, "request_id", id)
			status = http.StatusBadRequest
			c.Write(respInvalidCampaign)
			return gnet.None
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
		metrics.FilterThroughput.WithLabelValues("json").Inc()
		reqJSON := trackRequestPool.Get().(*TrackRequest)
		reqJSON.CampaignID = uuid.Nil
		reqJSON.UserID = ""
		reqJSON.Type = ""
		reqJSON.ClickID = ""
		reqJSON.Payload = reqJSON.Payload[:0]
		defer trackRequestPool.Put(reqJSON)

		if err := reqJSON.UnmarshalJSON(req.Body); err != nil {
			slog.Warn("invalid json body", "error", err, "request_id", id)
			status = http.StatusBadRequest
			c.Write(respInvalidJSON)
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

	evt := domain.EventPool.Get().(*domain.Event)
	evt.Reset()
	evt.ClickID = clickID
	evt.CampaignID = campaignID
	evt.UserID = userID
	evt.Type = eventType
	evt.Payload = append(evt.Payload[:0], payload...)
	evt.IP = ip
	evt.UA = ua

	if h.filterEngine != nil {
		if err := h.filterEngine.Check(context.Background(), evt); err != nil {
			if errors.Is(err, ErrEmergencyBreakerActive) {
				domain.EventPool.Put(evt)
				slog.Warn("event rejected: emergency breaker active", "request_id", id)
				metrics.FilterBlockedTotal.WithLabelValues("emergency_breaker").Inc()
				metrics.FilterDecisions.WithLabelValues("emergency_breaker").Inc()
				status = http.StatusServiceUnavailable
				c.Write(respEmergencyBreaker)
				return gnet.None
			} else if errors.Is(err, ErrRateLimitExceeded) {
				domain.EventPool.Put(evt)
				slog.Warn("event rejected: rate limit", "error", err, "request_id", id)
				metrics.FilterBlockedTotal.WithLabelValues("rate_limit").Inc()
				metrics.FilterDecisions.WithLabelValues("rate_limited").Inc()
				status = http.StatusTooManyRequests
				c.Write(respRateLimit)
				return gnet.None
			} else if errors.Is(err, ErrDuplicateEvent) {
				domain.EventPool.Put(evt)
				slog.Warn("event rejected: duplicate", "error", err, "request_id", id)
				metrics.FilterBlockedTotal.WithLabelValues("duplicate").Inc()
				metrics.FilterDecisions.WithLabelValues("duplicate").Inc()
				status = http.StatusConflict
				c.Write(respDuplicate)
				return gnet.None
			} else if errors.Is(err, ErrBudgetExhausted) {
				domain.EventPool.Put(evt)
				slog.Warn("event rejected: budget exhausted", "error", err, "request_id", id)
				metrics.FilterBlockedTotal.WithLabelValues("budget").Inc()
				metrics.FilterDecisions.WithLabelValues("budget_exhausted").Inc()
				status = http.StatusPaymentRequired
				c.Write(respBudget)
				return gnet.None
			} else if errors.Is(err, ErrPacingExhausted) {
				domain.EventPool.Put(evt)
				slog.Warn("event rejected: pacing exhausted", "error", err, "request_id", id)
				metrics.FilterBlockedTotal.WithLabelValues("pacing").Inc()
				metrics.FilterDecisions.WithLabelValues("pacing_limit").Inc()
				status = http.StatusTooManyRequests
				c.Write(respPacing)
				return gnet.None
			} else if errors.Is(err, ErrFreqLimitExceeded) {
				domain.EventPool.Put(evt)
				slog.Warn("event rejected: frequency limit", "error", err, "request_id", id)
				metrics.FilterBlockedTotal.WithLabelValues("freq").Inc()
				metrics.FilterDecisions.WithLabelValues("frequency_capped").Inc()
				status = http.StatusForbidden
				c.Write(respFreq)
				return gnet.None
			} else if errors.Is(err, ErrGeoBlocked) {
				domain.EventPool.Put(evt)
				slog.Warn("event rejected: geo blocked", "error", err, "request_id", id)
				metrics.FilterBlockedTotal.WithLabelValues("geo").Inc()
				metrics.FilterDecisions.WithLabelValues("geo_blocked").Inc()
				status = http.StatusForbidden
				c.Write(respGeo)
				return gnet.None
			} else if errors.Is(err, ErrFraudDetected) {
				slog.Warn("fraud detected: silent drop", "reason", evt.FraudReason, "request_id", id)
				metrics.FilterBlockedTotal.WithLabelValues("fraud").Inc()
				metrics.FilterDecisions.WithLabelValues("fraud").Inc()

				shard := h.sharder.GetShard(evt.CampaignID)
				rdb := h.rdbs[shard]

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

				rdbErr := rdb.XAdd(context.Background(), &redis.XAddArgs{
					Stream: h.fraudStream,
					MaxLen: int64(h.cfg.StreamMaxLen),
					Approx: true,
					Values: valSlice,
				}).Err()

				fraudValuesPool.Put(valSlicePtr)
				bufPool.Put(wCamp)
				bufPool.Put(wTime)

				if rdbErr != nil {
					slog.Error("failed to write to fraud stream", "error", rdbErr, "request_id", id)
				}
			} else {
				domain.EventPool.Put(evt)
				slog.Error("filter engine failure", "error", err, "request_id", id)
				status = http.StatusInternalServerError
				c.Write(respInternalError)
				return gnet.None
			}
		}
	}

	metrics.FilterDecisions.WithLabelValues("accepted").Inc()
	domain.EventPool.Put(evt)

	accept := unsafeString(req.Accept)
	if accept == "application/x-protobuf" {
		resp := trackResponsePool.Get().(*pb.TrackResponse)
		defer putTrackResponse(resp)

		if requestIDStr == "" {
			requestIDStr = unsafeString(wReqID.buf)
		}
		resp.RequestId = requestIDStr
		resp.Status = "accepted"

		respSize := resp.SizeVT()
		bufSlicePtr := responseBytesPool.Get().(*[]byte)
		bufSlice := *bufSlicePtr
		if cap(bufSlice) < 200+respSize {
			bufSlice = make([]byte, 200+respSize)
		} else {
			bufSlice = bufSlice[:200+respSize]
		}

		offset := copy(bufSlice, "HTTP/1.1 202 Accepted\r\nContent-Type: application/x-protobuf\r\nContent-Length: ")
		offset += copy(bufSlice[offset:], strconv.Itoa(respSize))
		offset += copy(bufSlice[offset:], "\r\nConnection: keep-alive\r\n\r\n")

		n, err := resp.MarshalToVT(bufSlice[offset : offset+respSize])
		if err != nil {
			*bufSlicePtr = bufSlice
			responseBytesPool.Put(bufSlicePtr)
			slog.Error("failed to marshal proto response", "error", err, "request_id", id)
			status = http.StatusInternalServerError
			c.Write(respInternalError)
			return gnet.None
		}
		outSlice := bufSlice[:offset+n]

		metrics.GnetBytesSent.Add(float64(len(outSlice)))
		metrics.GnetPacketsSent.Inc()
		c.Write(outSlice)
		*bufSlicePtr = bufSlice
		responseBytesPool.Put(bufSlicePtr)
	} else {
		if requestIDStr == "" {
			requestIDStr = unsafeString(wReqID.buf)
		}

		respSize := len(`{"request_id":"","status":"accepted"}`) + len(requestIDStr)
		bufSlicePtr := responseBytesPool.Get().(*[]byte)
		bufSlice := *bufSlicePtr
		if cap(bufSlice) < 200+respSize {
			bufSlice = make([]byte, 200+respSize)
		} else {
			bufSlice = bufSlice[:200+respSize]
		}

		offset := copy(bufSlice, "HTTP/1.1 202 Accepted\r\nContent-Type: application/json\r\nContent-Length: ")
		offset += copy(bufSlice[offset:], strconv.Itoa(respSize))
		offset += copy(bufSlice[offset:], "\r\nConnection: keep-alive\r\n\r\n")

		offset += copy(bufSlice[offset:], `{"request_id":"`)
		offset += copy(bufSlice[offset:], requestIDStr)
		offset += copy(bufSlice[offset:], `","status":"accepted"}`)

		outLen := offset
		metrics.GnetBytesSent.Add(float64(outLen))
		metrics.GnetPacketsSent.Inc()
		c.Write(bufSlice[:outLen])
		*bufSlicePtr = bufSlice
		responseBytesPool.Put(bufSlicePtr)
	}

	return gnet.None
}

func extractClientIPGnet(req *parsedHTTPRequest, remoteAddr net.Addr) string {
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

	addr := remoteAddr.String()
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
