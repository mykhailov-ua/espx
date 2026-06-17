package management

import (
	"context"
	"time"
)

const (
	workerBatchTimeout  = 2 * time.Minute
	workerDrainTimeout  = 30 * time.Second
	workerOutboxTimeout = 30 * time.Second
)

// workerContext bounds batch work so workers respect parent shutdown deadlines and avoid hanging forever.
func workerContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if deadline, ok := parent.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return context.WithCancel(parent)
		}
		if remaining < timeout {
			timeout = remaining
		}
	}
	return context.WithTimeout(parent, timeout)
}
