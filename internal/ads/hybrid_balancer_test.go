package ads

import (
	"fmt"
	"math/rand"
	"os"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestHybridBalancer_Empty(t *testing.T) {
	hb := NewHybridBalancer(10, 1000)
	campaign, shard := hb.SelectAndShard("user123", 0)
	assert.Nil(t, campaign)
	assert.Equal(t, 0, shard)

	hb.UpdateCampaigns(nil, 123, 3600)
	campaign, shard = hb.SelectAndShard("user123", 0)
	assert.Nil(t, campaign)
	assert.Equal(t, 0, shard)

	hb.UpdateCampaigns([]*CampaignMeta{}, 123, 3600)
	campaign, shard = hb.SelectAndShard("user123", 0)
	assert.Nil(t, campaign)
	assert.Equal(t, 0, shard)
}

func TestHybridBalancer_Correctness(t *testing.T) {
	c1 := &CampaignMeta{
		ID:                uuid.New(),
		BidMicro:          1000,
		CTR:               0.02,
		RemainingBudget:   5000,
		TotalBudget:       10000,
		PeakTrafficFactor: 0.1,
	}
	c2 := &CampaignMeta{
		ID:                uuid.New(),
		BidMicro:          2000,
		CTR:               0.05,
		RemainingBudget:   8000,
		TotalBudget:       10000,
		PeakTrafficFactor: 0.2,
	}

	hb := NewHybridBalancer(10, 1000)
	hb.UpdateCampaigns([]*CampaignMeta{c1, c2}, 1800, 3600)

	counts := make(map[uuid.UUID]int)
	iterations := 100000

	for i := 0; i < iterations; i++ {
		c, _ := hb.SelectAndShard(fmt.Sprintf("user-%d", i), 0)
		if c != nil {
			counts[c.ID]++
		}
	}

	assert.True(t, counts[c2.ID] > counts[c1.ID], "Higher eCPM/CTR campaign must be selected more frequently")
	assert.Equal(t, iterations, counts[c1.ID]+counts[c2.ID], "Total selections must equal iterations")
}

func TestHybridBalancer_ConcurrencyAndRaces(t *testing.T) {
	c1 := &CampaignMeta{
		ID:              uuid.New(),
		BidMicro:        1000,
		CTR:             0.01,
		RemainingBudget: 1000,
		TotalBudget:     1000,
	}
	c2 := &CampaignMeta{
		ID:              uuid.New(),
		BidMicro:        2000,
		CTR:             0.02,
		RemainingBudget: 1000,
		TotalBudget:     1000,
	}

	hb := NewHybridBalancer(10, 1000)
	hb.UpdateCampaigns([]*CampaignMeta{c1, c2}, 1800, 3600)

	var wg sync.WaitGroup
	workers := 16
	requestsPerWorker := 10000

	// Concurrent readers performing SelectAndShard.
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < requestsPerWorker; j++ {
				_, shard := hb.SelectAndShard(fmt.Sprintf("user-%d-%d", workerID, j), int64(j%2000))
				assert.True(t, shard >= 0 && shard < 10)
			}
		}(i)
	}

	// Concurrent writer continuously updating campaigns.
	stopWriter := make(chan struct{})
	go func() {
		ticker := time.NewTicker(1 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopWriter:
				return
			case <-ticker.C:
				c1.RemainingBudget = int64(rand.Intn(1000))
				c2.RemainingBudget = int64(rand.Intn(1000))
				hb.UpdateCampaigns([]*CampaignMeta{c1, c2}, time.Now().Unix()%3600, 3600)
			}
		}
	}()

	wg.Wait()
	close(stopWriter)
}

func BenchmarkHybridBalancer_SelectAndShard(b *testing.B) {
	c1 := &CampaignMeta{
		ID:                uuid.New(),
		BidMicro:          1000,
		CTR:               0.01,
		RemainingBudget:   5000,
		TotalBudget:       10000,
		PeakTrafficFactor: 0.1,
	}
	c2 := &CampaignMeta{
		ID:                uuid.New(),
		BidMicro:          2000,
		CTR:               0.02,
		RemainingBudget:   8000,
		TotalBudget:       10000,
		PeakTrafficFactor: 0.2,
	}

	hb := NewHybridBalancer(8, 5000)
	hb.UpdateCampaigns([]*CampaignMeta{c1, c2}, 1800, 3600)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = hb.SelectAndShard("user-123", int64(i%8000))
			i++
		}
	})
}

func TestHybridBalancer_Pprof(t *testing.T) {
	if os.Getenv("RUN_PPROF") != "true" {
		t.Skip("Skipping pprof CPU profiling hook")
	}

	c1 := &CampaignMeta{
		ID:                uuid.New(),
		BidMicro:          1000,
		CTR:               0.01,
		RemainingBudget:   5000,
		TotalBudget:       10000,
		PeakTrafficFactor: 0.1,
	}
	c2 := &CampaignMeta{
		ID:                uuid.New(),
		BidMicro:          2000,
		CTR:               0.02,
		RemainingBudget:   8000,
		TotalBudget:       10000,
		PeakTrafficFactor: 0.2,
	}

	hb := NewHybridBalancer(8, 5000)
	hb.UpdateCampaigns([]*CampaignMeta{c1, c2}, 1800, 3600)

	f, err := os.Create("cpu.prof")
	assert.NoError(t, err)
	defer f.Close()

	err = pprof.StartCPUProfile(f)
	assert.NoError(t, err)
	defer pprof.StopCPUProfile()

	start := time.Now()
	for time.Since(start) < 1*time.Second {
		for i := 0; i < 10000; i++ {
			_, _ = hb.SelectAndShard("user-pprof", int64(i%10000))
		}
	}
}

func TestHybridBalancer_EdgeCases(t *testing.T) {
	// Case 1: Nil campaign in UpdateCampaigns
	t.Run("NilCampaign", func(t *testing.T) {
		hb := NewHybridBalancer(10, 1000)
		assert.NotPanics(t, func() {
			hb.UpdateCampaigns([]*CampaignMeta{nil}, 1800, 3600)
		})
		campaign, shard := hb.SelectAndShard("user123", 100)
		assert.Nil(t, campaign)
		assert.Equal(t, 0, shard)
	})

	// Case 2: totalSeconds = 0 (NaN pacingFactor prevention)
	t.Run("ZeroTotalSeconds", func(t *testing.T) {
		hb := NewHybridBalancer(10, 1000)
		c := &CampaignMeta{
			ID:              uuid.New(),
			BidMicro:        1000,
			CTR:             0.02,
			RemainingBudget: 500,
			TotalBudget:     1000,
		}
		assert.NotPanics(t, func() {
			hb.UpdateCampaigns([]*CampaignMeta{c}, 100, 0)
		})
		campaign, _ := hb.SelectAndShard("user123", 100)
		_ = campaign
	})

	// Case 3: c.TotalBudget = 0 (NaN budgetRatio prevention)
	t.Run("ZeroTotalBudget", func(t *testing.T) {
		hb := NewHybridBalancer(10, 1000)
		c := &CampaignMeta{
			ID:              uuid.New(),
			BidMicro:        1000,
			CTR:             0.02,
			RemainingBudget: 500,
			TotalBudget:     0,
		}
		assert.NotPanics(t, func() {
			hb.UpdateCampaigns([]*CampaignMeta{c}, 100, 3600)
		})
		campaign, _ := hb.SelectAndShard("user123", 100)
		_ = campaign
	})

	// Case 4: maxRpsPerNode = 0 & hot traffic (prevent division by zero panic)
	t.Run("ZeroMaxRpsPerNode", func(t *testing.T) {
		hb := NewHybridBalancer(10, 0)
		c := &CampaignMeta{
			ID:              uuid.New(),
			BidMicro:        1000,
			CTR:             0.02,
			RemainingBudget: 500,
			TotalBudget:     1000,
		}
		hb.UpdateCampaigns([]*CampaignMeta{c}, 1800, 3600)
		assert.NotPanics(t, func() {
			campaign, shard := hb.SelectAndShard("user123", 100)
			assert.NotNil(t, campaign)
			assert.True(t, shard >= 0 && shard < 10)
		})
	})

	// Case 5: totalShards = 0 or negative (prevent negative jumpHash bounds panic)
	t.Run("ZeroOrNegativeTotalShards", func(t *testing.T) {
		hb := NewHybridBalancer(0, 1000)
		c := &CampaignMeta{
			ID:              uuid.New(),
			BidMicro:        1000,
			CTR:             0.02,
			RemainingBudget: 500,
			TotalBudget:     1000,
		}
		hb.UpdateCampaigns([]*CampaignMeta{c}, 1800, 3600)
		assert.NotPanics(t, func() {
			campaign, shard := hb.SelectAndShard("user123", 100)
			assert.NotNil(t, campaign)
			assert.Equal(t, 0, shard)
		})

		hbNeg := NewHybridBalancer(-5, 1000)
		hbNeg.UpdateCampaigns([]*CampaignMeta{c}, 1800, 3600)
		assert.NotPanics(t, func() {
			campaign, shard := hbNeg.SelectAndShard("user123", 100)
			assert.NotNil(t, campaign)
			assert.Equal(t, 0, shard)
		})
	})
}

