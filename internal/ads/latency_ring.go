package ads

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

const defaultLatencyRingCap = 4096

// LatencyRing buffers request durations off the hot path to avoid Prometheus CAS in gnet.
type LatencyRing struct {
	slots    []atomic.Uint64
	mask     uint64
	seq      atomic.Uint64
	flushSeq atomic.Uint64
}

// NewLatencyRing allocates a power-of-two ring for monotonic latency samples.
func NewLatencyRing(capacity int) *LatencyRing {
	if capacity < 2 || capacity&(capacity-1) != 0 {
		capacity = defaultLatencyRingCap
	}
	return &LatencyRing{
		slots: make([]atomic.Uint64, capacity),
		mask:  uint64(capacity - 1),
	}
}

// Capacity returns the number of ring slots.
func (r *LatencyRing) Capacity() int {
	if r == nil {
		return 0
	}
	return int(r.mask + 1)
}

// RecordMono stores elapsed monotonic nanoseconds from a request start timestamp.
func (r *LatencyRing) RecordMono(startMono int64) {
	if r == nil || startMono <= 0 {
		return
	}
	elapsed := monotonicNano() - startMono
	if elapsed < 0 {
		return
	}
	next := r.seq.Add(1)
	r.slots[(next-1)&r.mask].Store(uint64(elapsed))
}

// FlushTo exports buffered samples to Prometheus during metrics scrape only.
func (r *LatencyRing) FlushTo(observer prometheus.Observer) int {
	if r == nil || observer == nil {
		return 0
	}
	head := r.seq.Load()
	tail := r.flushSeq.Load()
	if head <= tail {
		return 0
	}

	capacity := r.mask + 1
	if head-tail > capacity {
		tail = head - capacity
	}

	n := 0
	for i := tail; i < head; i++ {
		ns := r.slots[i&r.mask].Load()
		if ns == 0 {
			continue
		}
		observer.Observe(float64(ns) / nanosPerSecond)
		n++
	}
	r.flushSeq.Store(head)
	return n
}

// Pending returns the number of samples not yet flushed to Prometheus.
func (r *LatencyRing) Pending() uint64 {
	if r == nil {
		return 0
	}
	head := r.seq.Load()
	tail := r.flushSeq.Load()
	if head <= tail {
		return 0
	}
	if head-tail > r.mask+1 {
		return r.mask + 1
	}
	return head - tail
}
