package management

import (
	"context"
	"log/slog"
	"time"

	"espx/internal/ads"
)

type PacingControllerWorker struct {
	svc         *Service
	syncWorkers []*ads.SyncWorker
}

func NewPacingControllerWorker(svc *Service, syncWorkers []*ads.SyncWorker) *PacingControllerWorker {
	return &PacingControllerWorker{
		svc:         svc,
		syncWorkers: syncWorkers,
	}
}

func (w *PacingControllerWorker) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.svc.ClosedLoopPacingController(ctx, w.syncWorkers); err != nil {
				slog.Error("closed-loop pacing controller run failed", "error", err)
			}
		}
	}
}
