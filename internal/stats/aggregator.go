package stats

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mykhailov-ua/ad-event-processor/internal/database/db"
)

const (
	// CampaignTTL defines how long we keep a campaign in memory without activity
	CampaignTTL = 2 * time.Hour
	// MaxWorkers defines concurrent database writers for stats
	MaxWorkers = 10
)

type Counters struct {
	Impressions atomic.Int64
	Clicks      atomic.Int64
	Conversions atomic.Int64
	LastSeen    atomic.Int64 // Unix timestamp
}

type flushTask struct {
	campaignID uuid.UUID
	imps       int64
	clicks     int64
	convs      int64
}

type Aggregator struct {
	repo     db.Querier
	data     sync.Map // map[uuid.UUID]*Counters
	flushInt time.Duration
	tasks    chan flushTask
}

func NewAggregator(repo db.Querier, flushInt time.Duration) *Aggregator {
	return &Aggregator{
		repo:     repo,
		flushInt: flushInt,
		tasks:    make(chan flushTask, 5000), // Buffered channel for worker pool
	}
}

func (a *Aggregator) getCounters(campaignID uuid.UUID) *Counters {
	val, _ := a.data.LoadOrStore(campaignID, &Counters{})
	c := val.(*Counters)
	c.LastSeen.Store(time.Now().Unix())
	return c
}

func (a *Aggregator) Increment(campaignID uuid.UUID, eventType string) {
	c := a.getCounters(campaignID)
	switch eventType {
	case "impression":
		c.Impressions.Add(1)
	case "click":
		c.Clicks.Add(1)
	case "conversion":
		c.Conversions.Add(1)
	}
}

func (a *Aggregator) Start(ctx context.Context) {
	// Start worker pool
	for i := 0; i < MaxWorkers; i++ {
		go a.worker(ctx)
	}

	go func() {
		ticker := time.NewTicker(a.flushInt)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				a.flush(context.Background())
				return
			case <-ticker.C:
				a.flush(ctx)
			}
		}
	}()
}

func (a *Aggregator) worker(ctx context.Context) {
	for task := range a.tasks {
		err := a.repo.UpdateCampaignStats(ctx, db.UpdateCampaignStatsParams{
			CampaignID:       pgtype.UUID{Bytes: task.campaignID, Valid: true},
			ImpressionsCount: task.imps,
			ClicksCount:      task.clicks,
			ConversionsCount: task.convs,
		})

		if err != nil {
			slog.Error("failed to update campaign stats", 
				"campaign_id", task.campaignID, 
				"error", err,
			)
		}
	}
}

func (a *Aggregator) flush(ctx context.Context) {
	now := time.Now().Unix()
	ttlThreshold := int64(CampaignTTL.Seconds())

	a.data.Range(func(key, value interface{}) bool {
		campaignID := key.(uuid.UUID)
		c := value.(*Counters)

		// 1. Memory Leak protection: Check TTL
		lastSeen := c.LastSeen.Load()
		if now-lastSeen > ttlThreshold {
			// Double check if there are pending stats before deleting
			if c.Impressions.Load() == 0 && c.Clicks.Load() == 0 && c.Conversions.Load() == 0 {
				a.data.Delete(key)
				return true
			}
		}

		// 2. Atomic swap for thread-safe counters reset
		impressions := c.Impressions.Swap(0)
		clicks := c.Clicks.Swap(0)
		conversions := c.Conversions.Swap(0)

		if impressions == 0 && clicks == 0 && conversions == 0 {
			return true
		}

		// 3. Offload DB write to worker pool (non-blocking)
		select {
		case a.tasks <- flushTask{
			campaignID: campaignID,
			imps:       impressions,
			clicks:     clicks,
			convs:      conversions,
		}:
		default:
			slog.Warn("stats task queue is full, dropping stats", "campaign_id", campaignID)
		}

		return true
	})
}
