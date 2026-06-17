package ads

import (
	"context"
	"errors"
	"net/http"

	"espx/internal/database"
	"espx/internal/metrics"
)

// filterRejectKind classifies filter errors into stable HTTP and metrics responses.
type filterRejectKind uint8

// Filter rejection categories mapped to HTTP status and metric labels.
const (
	filterRejectEmergencyBreaker filterRejectKind = iota
	filterRejectRateLimit
	filterRejectDuplicate
	filterRejectBudget
	filterRejectPacing
	filterRejectFreq
	filterRejectGeo
	filterRejectSchedule
	filterRejectCampaignNotFound
	filterRejectBidFloor
	filterRejectTimeout
	filterRejectFraud
	filterRejectInfra
)

// filterRejectSpec holds the HTTP response template for a rejection kind.
type filterRejectSpec struct {
	status      int
	body        string
	gnetResp    []byte
	metricLabel string
}

// filterRejectSpecs is the lookup table from rejection kind to client response.
var filterRejectSpecs = [...]filterRejectSpec{
	filterRejectEmergencyBreaker: {http.StatusServiceUnavailable, "service temporarily unavailable", respEmergencyBreaker, "emergency_breaker"},
	filterRejectRateLimit:        {http.StatusTooManyRequests, "rate limit exceeded", respRateLimit, "rate_limit"},
	filterRejectDuplicate:        {http.StatusConflict, "duplicate event", respDuplicate, "duplicate"},
	filterRejectBudget:           {http.StatusPaymentRequired, "budget exhausted", respBudget, "budget"},
	filterRejectPacing:           {http.StatusTooManyRequests, "pacing limit reached", respPacing, "pacing"},
	filterRejectFreq:             {http.StatusForbidden, "frequency limit reached", respFreq, "freq"},
	filterRejectGeo:              {http.StatusForbidden, "geo-targeting blocked", respGeo, "geo"},
	filterRejectSchedule:         {http.StatusForbidden, "outside delivery schedule", respSchedule, "schedule"},
	filterRejectCampaignNotFound: {http.StatusNotFound, "campaign not found", respCampaignNotFound, "campaign_not_found"},
	filterRejectBidFloor:         {http.StatusPaymentRequired, "bid floor not met", respBidFloorNotMet, "bid_floor"},
	filterRejectTimeout:          {http.StatusGatewayTimeout, "filter timeout", respFilterTimeout, "filter_timeout"},
	filterRejectFraud:            {http.StatusAccepted, "", nil, "fraud"},
	filterRejectInfra:            {http.StatusServiceUnavailable, "service unavailable", respInfraUnavailable, "infra_unavailable"},
}

// classifyFilterErr maps domain filter errors to a stable rejection kind.
func classifyFilterErr(err error) (filterRejectKind, bool) {
	switch {
	case errors.Is(err, ErrEmergencyBreakerActive):
		return filterRejectEmergencyBreaker, true
	case errors.Is(err, ErrRateLimitExceeded):
		return filterRejectRateLimit, true
	case errors.Is(err, ErrDuplicateEvent):
		return filterRejectDuplicate, true
	case errors.Is(err, ErrBudgetExhausted):
		return filterRejectBudget, true
	case errors.Is(err, ErrPacingExhausted):
		return filterRejectPacing, true
	case errors.Is(err, ErrFreqLimitExceeded):
		return filterRejectFreq, true
	case errors.Is(err, ErrGeoBlocked):
		return filterRejectGeo, true
	case errors.Is(err, ErrScheduleBlocked):
		return filterRejectSchedule, true
	case errors.Is(err, ErrCampaignNotFound):
		return filterRejectCampaignNotFound, true
	case errors.Is(err, ErrBidFloorNotMet):
		return filterRejectBidFloor, true
	case errors.Is(err, context.DeadlineExceeded):
		return filterRejectTimeout, true
	case errors.Is(err, ErrFraudDetected):
		return filterRejectFraud, true
	case isInfraFilterErr(err):
		return filterRejectInfra, true
	default:
		return 0, false
	}
}

// isInfraFilterErr treats Redis circuit and network faults as retryable infra failures.
func isInfraFilterErr(err error) bool {
	if errors.Is(err, database.ErrRedisCircuitOpen) {
		return true
	}
	return database.IsNetworkOrSystemError(err)
}

// recordFilterReject increments pre-bound gnet track counters for a rejection kind.
func (m *preboundTrackMetrics) recordFilterReject(kind filterRejectKind) {
	switch kind {
	case filterRejectEmergencyBreaker:
		m.blockedEmergencyBreaker.Inc()
		m.decisionEmergencyBreaker.Inc()
	case filterRejectRateLimit:
		m.blockedRateLimit.Inc()
		m.decisionRateLimited.Inc()
	case filterRejectDuplicate:
		m.blockedDuplicate.Inc()
		m.decisionDuplicate.Inc()
	case filterRejectBudget:
		m.blockedBudget.Inc()
		m.decisionBudgetExhausted.Inc()
	case filterRejectPacing:
		m.blockedPacing.Inc()
		m.decisionPacingLimit.Inc()
	case filterRejectFreq:
		m.blockedFreq.Inc()
		m.decisionFrequencyCapped.Inc()
	case filterRejectGeo:
		m.blockedGeo.Inc()
		m.decisionGeoBlocked.Inc()
	case filterRejectSchedule:
		m.blockedSchedule.Inc()
		m.decisionScheduleBlocked.Inc()
	case filterRejectCampaignNotFound:
		m.blockedCampaignNotFound.Inc()
		m.decisionCampaignNotFound.Inc()
	case filterRejectBidFloor:
		m.blockedBidFloor.Inc()
		m.decisionBidFloor.Inc()
	case filterRejectTimeout:
		m.blockedFilterTimeout.Inc()
		m.decisionFilterTimeout.Inc()
	case filterRejectFraud:
		m.blockedFraud.Inc()
		m.decisionFraud.Inc()
	case filterRejectInfra:
		m.blockedInfra.Inc()
		m.decisionInfraUnavailable.Inc()
	}
}

// recordHTTPFilterReject increments stdlib HTTP track blocked counters.
func recordHTTPFilterReject(kind filterRejectKind) {
	metrics.FilterBlockedTotal.WithLabelValues(filterRejectSpecs[kind].metricLabel).Inc()
}
