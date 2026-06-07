package rtb

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestAuctionBasic(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)

	c1 := uuid.New()
	c2 := uuid.New()
	c3 := uuid.New()

	campaigns := []CampaignData{
		{
			ID:           c1,
			BidFloor:     150,
			DeviceMask:   2,
			CategoryMask: 1,
			GeoHashVal:   10,
			Weight:       10,
			Budget:       1000,
		},
		{
			ID:           c2,
			BidFloor:     250,
			DeviceMask:   2,
			CategoryMask: 1,
			GeoHashVal:   10,
			Weight:       20,
			Budget:       2000,
		},
		{
			ID:           c3,
			BidFloor:     80,
			DeviceMask:   2,
			CategoryMask: 1,
			GeoHashVal:   10,
			Weight:       5,
			Budget:       1000,
		},
	}

	reg.UpdateCampaigns(campaigns)

	req := &BidRequest{
		DeviceType:   2,
		CategoryMask: 1,
		GeoHash:      10,
		MinBid:       100,
	}

	res, ok := reg.RunAuction(req)
	if !ok {
		t.Fatalf("expected auction to succeed")
	}

	if res.CampaignID != c2 {
		t.Errorf("expected winner %s, got %s", c2, res.CampaignID)
	}
	if res.Price != 150 {
		t.Errorf("expected price 150, got %d", res.Price)
	}

	budgetRemaining := store.GetBudget(c2)
	if budgetRemaining != 1850 {
		t.Errorf("expected budget 1850, got %d", budgetRemaining)
	}
}

func TestAuctionTopKHeap(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	n := 200
	campaigns := make([]CampaignData, n)

	for i := 0; i < n; i++ {
		campaigns[i] = CampaignData{
			ID:           uuid.New(),
			BidFloor:     int64(100 + i),
			DeviceMask:   1,
			CategoryMask: 1,
			GeoHashVal:   5,
			Weight:       uint32(i),
			Budget:       10000,
		}
	}

	reg.UpdateCampaigns(campaigns)

	req := &BidRequest{
		DeviceType:   1,
		CategoryMask: 1,
		GeoHash:      5,
		MinBid:       50,
	}

	res, ok := reg.RunAuction(req)
	if !ok {
		t.Fatalf("expected auction to succeed")
	}

	expectedWinnerID := campaigns[n-1].ID
	if res.CampaignID != expectedWinnerID {
		t.Errorf("expected winner %s, got %s", expectedWinnerID, res.CampaignID)
	}
	if res.Price != 298 {
		t.Errorf("expected price 298, got %d", res.Price)
	}
}

// TestAuctionStressSimulateNetworkAndDB simulates network jitters and database updates
// under concurrent load to verify zero races, budget overspend safety, and deadlock immunity.
func TestAuctionStressSimulateNetworkAndDB(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	n := 100
	campaigns := make([]CampaignData, n)

	for i := 0; i < n; i++ {
		campaigns[i] = CampaignData{
			ID:           uuid.New(),
			BidFloor:     int64(100 + i),
			DeviceMask:   1,
			CategoryMask: 1,
			GeoHashVal:   uint32(i % 16),
			Weight:       uint32(i),
			Budget:       5000,
		}
	}
	reg.UpdateCampaigns(campaigns)

	var wg sync.WaitGroup
	workers := 12
	iterations := 500

	// Simulating concurrent request streams with random network latency jitters
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(int64(workerID)))
			for i := 0; i < iterations; i++ {
				// Jitter simulator (1-5 microseconds network delay simulation)
				if i%50 == 0 {
					time.Sleep(time.Duration(rnd.Intn(5)+1) * time.Microsecond)
				}
				req := &BidRequest{
					DeviceType:   1,
					CategoryMask: 1,
					GeoHash:      uint32(rnd.Intn(16)),
					MinBid:       int64(100 + rnd.Intn(40)),
				}
				_, _ = reg.RunAuction(req)
			}
		}(w)
	}

	// Simulating slow/delayed background DB updates executing RCU pointer swaps concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		rnd := rand.New(rand.NewSource(999))
		for i := 0; i < 20; i++ {
			// Simulate database sync latency (2-10 milliseconds wait)
			time.Sleep(time.Duration(rnd.Intn(8)+2) * time.Millisecond)

			updated := make([]CampaignData, n)
			for j := 0; j < n; j++ {
				updated[j] = campaigns[j]
				updated[j].Weight += uint32(i)
			}
			reg.UpdateCampaigns(updated)
		}
	}()

	wg.Wait()

	// Enforce that atomic CAS prevented campaign budgets from ever going below 0 (no overspend)
	for i := 0; i < n; i++ {
		b := store.GetBudget(campaigns[i].ID)
		if b < 0 {
			t.Errorf("campaign %s budget overspent: %d", campaigns[i].ID, b)
		}
	}
}

func BenchmarkAuction(b *testing.B) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	n := 1000
	campaigns := make([]CampaignData, n)

	for i := 0; i < n; i++ {
		deviceMask := uint8(1 << (i % 3))
		campaigns[i] = CampaignData{
			ID:           uuid.New(),
			BidFloor:     int64(100 + (i % 500)),
			DeviceMask:   deviceMask,
			CategoryMask: uint64(1 << (i % 8)),
			GeoHashVal:   uint32(i),
			Weight:       uint32(i),
			Budget:       1000000000,
		}
	}
	reg.UpdateCampaigns(campaigns)

	req := &BidRequest{
		DeviceType:   2,
		CategoryMask: 4,
		GeoHash:      2,
		MinBid:       150,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = reg.RunAuction(req)
	}
}

func BenchmarkAuctionHighDensity(b *testing.B) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	n := 1000
	campaigns := make([]CampaignData, n)

	for i := 0; i < n; i++ {
		campaigns[i] = CampaignData{
			ID:           uuid.New(),
			BidFloor:     int64(100 + i),
			DeviceMask:   1,
			CategoryMask: 1,
			GeoHashVal:   5,
			Weight:       uint32(i),
			Budget:       1000000000,
		}
	}
	reg.UpdateCampaigns(campaigns)

	req := &BidRequest{
		DeviceType:   1,
		CategoryMask: 1,
		GeoHash:      5,
		MinBid:       50,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = reg.RunAuction(req)
	}
}

func BenchmarkUpdateCampaigns(b *testing.B) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	n := 1000
	campaigns := make([]CampaignData, n)

	for i := 0; i < n; i++ {
		campaigns[i] = CampaignData{
			ID:           uuid.New(),
			BidFloor:     int64(100 + i),
			DeviceMask:   1,
			CategoryMask: 1,
			GeoHashVal:   uint32(i),
			Weight:       uint32(i),
			Budget:       10000,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reg.UpdateCampaigns(campaigns)
	}
}

func TestAuctionInvalidInput(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)

	c1 := uuid.New()
	campaigns := []CampaignData{
		{
			ID:           c1,
			BidFloor:     150,
			DeviceMask:   2,
			CategoryMask: 1,
			GeoHashVal:   10,
			Weight:       10,
			Budget:       1000,
		},
	}
	reg.UpdateCampaigns(campaigns)

	// Case 1: Nil request
	if _, ok := reg.RunAuction(nil); ok {
		t.Error("expected nil request to be rejected")
	}

	// Case 2: Negative MinBid
	req := &BidRequest{
		DeviceType:   2,
		CategoryMask: 1,
		GeoHash:      10,
		MinBid:       -500,
	}
	if _, ok := reg.RunAuction(req); ok {
		t.Error("expected negative MinBid to be rejected")
	}

	// Verify budget was not modified
	if budget := store.GetBudget(c1); budget != 1000 {
		t.Errorf("expected budget to remain 1000, got %d", budget)
	}
}
