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
	CampaignTTL = 2 * time.Hour
)

type Counters struct {
	Impressions atomic.Int64
	Clicks      atomic.Int64
	Conversions atomic.Int64
	LastSeen    atomic.Int64
}

type flushTask struct {
	campaignID uuid.UUID
	imps       int64
	clicks     int64
	convs      int64
}

type Aggregator struct {
	repo     db.Querier
	data     sync.Map
	flushInt time.Duration
	tasks      chan flushTask
	maxWorkers int
	wg         sync.WaitGroup
}

func NewAggregator(repo db.Querier, flushInt time.Duration, maxWorkers int) *Aggregator {
	return &Aggregator{
		repo:       repo,
		flushInt:   flushInt,
		tasks:      make(chan flushTask, 5000),
		maxWorkers: maxWorkers,
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
	for i := 0; i < a.maxWorkers; i++ {
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
	
	const batchSize = 1000
	const maxWait = 100 * time.Millisecond
	
	batch := make([]flushTask, 0, batchSize)
	timer := time.NewTimer(maxWait)
	defer timer.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		a.doBatchFlush(batch)
		batch = batch[:0]
	}

	for {
		select {
		case task, ok := <-a.tasks:
			if !ok {
				flush()
				return
			}
			batch = append(batch, task)
			if len(batch) >= batchSize {
				flush()
				timer.Reset(maxWait)
			}
		case <-timer.C:
			flush()
			timer.Reset(maxWait)
		}
	}
}

func (a *Aggregator) doBatchFlush(batch []flushTask) {
	campaignIDs := make([]pgtype.UUID, len(batch))
	imps := make([]int64, len(batch))
	clicks := make([]int64, len(batch))
	convs := make([]int64, len(batch))

	for i, t := range batch {
		campaignIDs[i] = pgtype.UUID{Bytes: t.campaignID, Valid: true}
		imps[i] = t.imps
		clicks[i] = t.clicks
		convs[i] = t.convs
	}

	var err error
	waitTime := initialWait

	for i := 0; i <= maxRetries; i++ {
		dbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = a.repo.UpdateCampaignStatsBatch(dbCtx, db.UpdateCampaignStatsBatchParams{
			CampaignIds: campaignIDs,
			Impressions: imps,
			Clicks:      clicks,
			Conversions: convs,
		})
		cancel()

		if err == nil {
			if i > 0 {
				slog.Info("successfully updated campaign stats batch after retry", 
					"size", len(batch), 
					"attempts", i+1)
			}
			return
		}

		if i < maxRetries {
			slog.Warn("failed to update campaign stats batch, retrying...",
				"size", len(batch),
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

	slog.Error("all retries failed for campaign stats batch, data lost", 
		"size", len(batch), 
		"error", err,
	)
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

		lastSeen := c.LastSeen.Load()
		if now-lastSeen > ttlThreshold {
			if c.Impressions.Load() == 0 && c.Clicks.Load() == 0 && c.Conversions.Load() == 0 {
				a.data.Delete(key)
				return true
			}
		}

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

		a.tasks <- task

		return true
	})
}
