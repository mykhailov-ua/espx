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
	maxRetries    = 3
	initialWait   = 100 * time.Millisecond
	maxWait       = 2 * time.Second
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
	wg       sync.WaitGroup
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
		a.wg.Add(1)
		go a.worker()
	}

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		ticker := time.NewTicker(a.flushInt)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				a.flush(true)
				close(a.tasks)
				return
			case <-ticker.C:
				a.flush(false)
			}
		}
	}()
}

func (a *Aggregator) worker() {
	defer a.wg.Done()
	for task := range a.tasks {
		var err error
		waitTime := initialWait

		for i := 0; i <= maxRetries; i++ {
			dbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = a.repo.UpdateCampaignStats(dbCtx, db.UpdateCampaignStatsParams{
				CampaignID:       pgtype.UUID{Bytes: task.campaignID, Valid: true},
				ImpressionsCount: task.imps,
				ClicksCount:      task.clicks,
				ConversionsCount: task.convs,
			})
			cancel()

			if err == nil {
				if i > 0 {
					slog.Info("successfully updated campaign stats after retry", 
						"campaign_id", task.campaignID, 
						"attempts", i+1)
				}
				break
			}

			if i < maxRetries {
				slog.Warn("failed to update campaign stats, retrying...",
					"campaign_id", task.campaignID,
					"error", err,
					"attempt", i+1,
					"wait", waitTime)
				time.Sleep(waitTime)
				waitTime *= 2
				if waitTime > maxWait {
					waitTime = maxWait
				}
			}
		}

		if err != nil {
			slog.Error("all retries failed for campaign stats, data lost", 
				"campaign_id", task.campaignID, 
				"error", err,
			)
		}
	}
}

func (a *Aggregator) Wait() {
	a.wg.Wait()
}

func (a *Aggregator) flush(isShutdown bool) {
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

		task := flushTask{
			campaignID: campaignID,
			imps:       impressions,
			clicks:     clicks,
			convs:      conversions,
		}

		// 3. Offload DB write to worker pool (blocking to provide backpressure)
		a.tasks <- task

		return true
	})
}
