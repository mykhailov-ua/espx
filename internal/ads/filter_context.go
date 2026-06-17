package ads

import (
	"context"
	"time"
)

// filterDeadlineKey tags context with a monotonic filter deadline independent of wall clock.
type filterDeadlineKey struct{}

// attachFilterDeadline carries a monotonic deadline so filter checks share one timeout budget.
func attachFilterDeadline(ctx context.Context, timeout time.Duration) context.Context {
	if timeout <= 0 {
		return ctx
	}
	deadlineMono := monotonicNano() + timeout.Nanoseconds()
	return context.WithValue(ctx, filterDeadlineKey{}, deadlineMono)
}

// filterDeadlineMonoFromContext reads the monotonic deadline stored by attachFilterDeadline.
func filterDeadlineMonoFromContext(ctx context.Context) (int64, bool) {
	if ctx == nil {
		return 0, false
	}
	d, ok := ctx.Value(filterDeadlineKey{}).(int64)
	return d, ok
}

// filterDeadlineExceeded reports whether the shared filter deadline has elapsed.
func filterDeadlineExceeded(ctx context.Context) bool {
	if d, ok := filterDeadlineMonoFromContext(ctx); ok {
		return monotonicNano() > d
	}
	return false
}

// filterDeadlineRemaining returns time left before the filter deadline expires.
func filterDeadlineRemaining(ctx context.Context) (time.Duration, bool) {
	d, ok := filterDeadlineMonoFromContext(ctx)
	if !ok {
		return 0, false
	}
	rem := d - monotonicNano()
	if rem <= 0 {
		return 0, true
	}
	return time.Duration(rem), true
}
