package management

import (
	"context"
	"log/slog"
	"time"

	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
)

// PacingControllerWorker executes periodic evaluation of the closed-loop pacing logic in a background goroutine.
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

// Start initiates the execution ticker loop to regulate campaign spend velocities at designated intervals.
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
