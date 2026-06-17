package management

import (
	"context"
	"log/slog"
	"strings"
	"time"
)

// ScheduleWorker drives automatic pause and resume when campaigns enter or leave their delivery windows.
type ScheduleWorker struct {
	svc      *Service
	interval time.Duration
}

// NewScheduleWorker wires the schedule worker to the management service with a one-minute tick interval.
func NewScheduleWorker(svc *Service) *ScheduleWorker {
	return &ScheduleWorker{svc: svc, interval: time.Minute}
}

// Start runs the schedule tick loop until the context is cancelled or the database pool closes.
func (w *ScheduleWorker) Start(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.svc.ProcessScheduleTick(ctx); err != nil {
				if strings.Contains(err.Error(), "closed pool") {
					return
				}
				slog.Error("schedule worker tick failed", "error", err)
			}
		}
	}
}
