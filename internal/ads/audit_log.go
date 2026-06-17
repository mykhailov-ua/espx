package ads

import (
	"sync/atomic"

	"espx/internal/ads/pb"
	"espx/internal/metrics"
	"espx/pkg/logger"

	"github.com/google/uuid"
)

// auditLogSampleMaskDefault inherits the Lua metrics downsampling rate for audit logs.
const auditLogSampleMaskDefault = luaMetricsSampleMask

// auditLogSampleMaskFromConfig maps config to audit log sampling mask.
func auditLogSampleMaskFromConfig(cfgVal int) uint64 {
	return histogramSampleMaskFromConfig(cfgVal)
}

// auditLogPriority assigns higher logger priority to billable events during disk pressure.
func auditLogPriority(eventType string) uint8 {
	switch eventType {
	case "click", "conversion":
		return 1
	default:
		return 0
	}
}

// writeAuditLog emits a sampled protobuf audit record for accepted track events.
func writeAuditLog(
	l *logger.Logger,
	seq *atomic.Uint64,
	sampleMask uint64,
	shardID int,
	timestampUnix int64,
	campaignID uuid.UUID,
	clickID, eventType string,
) {
	if l == nil {
		return
	}
	priority := auditLogPriority(eventType)
	if priority == 0 {
		if !shouldSampleHistogram(seq.Add(1), sampleMask) {
			return
		}
	}

	rec := adLogRecordPool.Get().(*pb.AdLogRecord)
	rec.TimestampUnix = timestampUnix
	if cap(rec.CampaignId) < 16 {
		rec.CampaignId = make([]byte, 16)
	} else {
		rec.CampaignId = rec.CampaignId[:16]
	}
	copy(rec.CampaignId, campaignID[:])
	rec.ClickId = UnsafeBytes(clickID)
	rec.EventType = UnsafeBytes(eventType)
	rec.Priority = uint32(priority)

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
		if !l.WriteToShard(shardID, priority, buf[:n]) {
			metrics.HandlerLogDropTotal.Inc()
		}
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
