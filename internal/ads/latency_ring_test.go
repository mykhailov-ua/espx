package ads

import (
	"sync"
	"testing"

	"espx/internal/config"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// Test helper type for sumObserver scenarios.
type sumObserver struct {
	mu    sync.Mutex
	sum   float64
	count int
}

func (o *sumObserver) Observe(v float64) {
	o.mu.Lock()
	o.sum += v
	o.count++
	o.mu.Unlock()
}

func (o *sumObserver) stats() (sum float64, count int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.sum, o.count
}

// Guards latency ring records samples and flushes to observer on demand.
func TestLatencyRing_recordAndFlush(t *testing.T) {
	ring := NewLatencyRing(8)
	obs := &sumObserver{}

	ring.slots[0].Store(uint64(10 * nanosPerSecond))
	ring.seq.Store(1)
	ring.flushSeq.Store(0)

	n := ring.FlushTo(obs)
	require.Equal(t, 1, n)
	_, count := obs.stats()
	require.Equal(t, 1, count)
	require.Equal(t, uint64(1), ring.flushSeq.Load())
	require.Equal(t, uint64(0), ring.Pending())
}

// Guards monotonic record path stores elapsed time without wall clock drift.
func TestLatencyRing_recordMono(t *testing.T) {
	ring := NewLatencyRing(16)
	start := monotonicNano() - 100
	ring.RecordMono(start)
	require.Equal(t, uint64(1), ring.seq.Load())
	require.Equal(t, uint64(1), ring.Pending())

	obs := &sumObserver{}
	n := ring.FlushTo(obs)
	require.Equal(t, 1, n)
	_, count := obs.stats()
	require.Equal(t, 1, count)
}

// Guards lagged ring flush drops oldest samples instead of blocking ingest.
func TestLatencyRing_flushDropsOldestWhenLagged(t *testing.T) {
	ring := NewLatencyRing(4)
	for i := uint64(1); i <= 10; i++ {
		ring.slots[i&ring.mask].Store(i * nanosPerSecond)
		ring.seq.Store(i)
	}

	obs := &sumObserver{}
	n := ring.FlushTo(obs)
	require.Equal(t, 4, n)
	require.Equal(t, uint64(10), ring.flushSeq.Load())
}

// Guards concurrent record and flush on latency ring stay consistent.
func TestLatencyRing_concurrentRecordFlush(t *testing.T) {
	ring := NewLatencyRing(1024)
	obs := &sumObserver{}
	var wg sync.WaitGroup

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := monotonicNano()
			for i := 0; i < 2000; i++ {
				ring.RecordMono(start)
			}
		}()
	}
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				ring.FlushTo(obs)
			}
		}()
	}
	wg.Wait()
}

// Guards handler recordMetrics increments status counters and feeds the latency ring.
func TestAdsPacketHandler_recordMetrics_countersAndRing(t *testing.T) {
	cfg := &config.Config{MaxRequestBodySize: 1024 * 1024}
	h := NewAdsPacketHandler(cfg, &mockRegistry{}, nil, nil, nil, NewJumpHashSharder(1), "fraud", nil)
	require.NotNil(t, h.trackLatencyRing)

	before := testutil.ToFloat64(h.trackStatusCounters[202])
	start := monotonicNano()
	const n = 32
	for i := 0; i < n; i++ {
		h.recordMetrics(start, 202)
	}
	after := testutil.ToFloat64(h.trackStatusCounters[202])
	require.Equal(t, before+float64(n), after)
	require.GreaterOrEqual(t, h.trackLatencyRing.Pending(), uint64(n-1))

	obs := &sumObserver{}
	flushed := h.trackLatencyRing.FlushTo(obs)
	require.GreaterOrEqual(t, flushed, n-1)
	_, count := obs.stats()
	require.GreaterOrEqual(t, count, n-1)
}

// Guards metrics gather flushes latency ring so samples are not lost.
func TestAdsPacketHandler_metricsFlushBeforeGather(t *testing.T) {
	cfg := &config.Config{MaxRequestBodySize: 1024 * 1024}
	h := NewAdsPacketHandler(cfg, &mockRegistry{}, nil, nil, nil, NewJumpHashSharder(1), "fraud", nil)
	h.trackDurationObserver = &sumObserver{}

	start := monotonicNano()
	h.recordMetrics(start, 202)
	require.Equal(t, uint64(1), h.trackLatencyRing.Pending())

	n := h.trackLatencyRing.FlushTo(h.trackDurationObserver)
	require.Equal(t, 1, n)
}

// Tracks monotonic latency ring record cost on request path.
func BenchmarkLatencyRing_RecordMono(b *testing.B) {
	ring := NewLatencyRing(defaultLatencyRingCap)
	start := monotonicNano()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.RecordMono(start)
	}
}

// Tracks latency ring record plus flush cost for scrape alignment.
func BenchmarkLatencyRing_RecordAndFlush(b *testing.B) {
	ring := NewLatencyRing(defaultLatencyRingCap)
	obs := &sumObserver{}
	start := monotonicNano()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.RecordMono(start)
		if i%128 == 0 {
			ring.FlushTo(obs)
		}
	}
}
