package ads

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/repository"
)

var (
	CampaignTTL = 2 * time.Hour
)

// Counters holds atomic tallies for a single campaign's events in memory.
type Counters struct {
	Impressions atomic.Int64
	Clicks      atomic.Int64
	Conversions atomic.Int64
	LastSeen    atomic.Int64 // Unix timestamp for TTL-based memory cleanup.
}

type flushTask struct {
	campaignID uuid.UUID
	imps       int64
	clicks     int64
	convs      int64
}

// Aggregator provides high-performance, thread-safe event counting in RAM.
// It periodicially flushes aggregated totals to the database to minimize I/O.
type Aggregator struct {
	repo         repository.Querier
	data         sync.Map // Map[uuid.UUID]*Counters
	flushInt     time.Duration
	writeTimeout time.Duration
	tasks        chan flushTask
	maxWorkers   int
	wg           sync.WaitGroup
}

func NewAggregator(repo repository.Querier, flushInt, writeTimeout time.Duration, maxWorkers int) *Aggregator {
	return &Aggregator{
		repo:         repo,
		flushInt:     flushInt,
		writeTimeout: writeTimeout,
		tasks:        make(chan flushTask, maxWorkers*2),
		maxWorkers:   maxWorkers,
	}
}

func (a *Aggregator) getCounters(campaignID uuid.UUID) *Counters {
	val, _ := a.data.LoadOrStore(campaignID, &Counters{})
	c := val.(*Counters)
	c.LastSeen.Store(time.Now().Unix())
	return c
}

func (a *Aggregator) Increment(campaignID uuid.UUID, eventType string) {
	StatsIncrements.Inc()
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
				return
			case <-ticker.C:
				a.flush()
			}
		}
	}()
}

func (a *Aggregator) Stop() {
	slog.Info("stats aggregator: performing final flush")
	a.flush()
	close(a.tasks)
	a.Wait()
	slog.Info("stats aggregator: stopped")
}

// worker processes flush tasks from the internal queue.
// It groups individual campaign updates into larger SQL batches for efficiency.
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
	waitTime := InitialWait

	for i := 0; i <= MaxRetries; i++ {
		dbCtx, cancel := context.WithTimeout(context.Background(), a.writeTimeout)

		start := time.Now()
		err = a.repo.UpdateCampaignStatsBatch(dbCtx, repository.UpdateCampaignStatsBatchParams{
			CampaignIds: campaignIDs,
			Impressions: imps,
			Clicks:      clicks,
			Conversions: convs,
		})
		duration := time.Since(start).Seconds()
		cancel()

		if err == nil {
			DbWriteDuration.WithLabelValues("batch_upsert").Observe(duration)
			if i > 0 {
				slog.Info("successfully updated campaign stats batch after retry",
					"size", len(batch),
					"attempts", i+1)
			}
			return
		}

		if i < MaxRetries {
			slog.Warn("failed to update campaign stats batch, retrying...",
				"size", len(batch),
				"error", err,
				"attempt", i+1,
				"wait", waitTime)
			time.Sleep(waitTime)
			waitTime *= 2
			if waitTime > MaxWait {
				waitTime = MaxWait
			}
		}
	}

	DbWriteErrors.WithLabelValues("batch_upsert").Inc()
	slog.Error("all retries failed for campaign stats batch, data lost",
		"size", len(batch),
		"error", err,
	)
}

func (a *Aggregator) Wait() {
	a.wg.Wait()
}

// flush extracts non-zero counters from the in-memory map and sends them to workers.
// It also cleans up old campaigns that haven't been seen for CampaignTTL.
func (a *Aggregator) flush() {
	now := time.Now().Unix()
	ttlThreshold := int64(CampaignTTL.Seconds())

	activeCount := 0
	a.data.Range(func(key, value interface{}) bool {
		activeCount++
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

	ActiveCampaignsCount.Set(float64(activeCount))
}
