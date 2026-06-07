package rtb

import (
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

// AlignedBudget prevents false sharing during concurrent atomic updates.
type AlignedBudget struct {
	Value int64
	_     [7]int64
}

// BudgetStore uses a flat backing array to avoid GC scanning and pointer chasing.
type BudgetStore struct {
	mu      sync.RWMutex
	slots   map[uuid.UUID]uint32
	budgets []AlignedBudget
}

func NewBudgetStore() *BudgetStore {
	return &BudgetStore{
		slots:   make(map[uuid.UUID]uint32),
		budgets: make([]AlignedBudget, 0, 10000),
	}
}

func (s *BudgetStore) GetOrAllocateSlot(id uuid.UUID, initialBudget int64) uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()

	if idx, exists := s.slots[id]; exists {
		return idx
	}

	idx := uint32(len(s.budgets))
	s.budgets = append(s.budgets, AlignedBudget{Value: initialBudget})
	s.slots[id] = idx
	return idx
}

func (s *BudgetStore) LoadBudget(idx uint32) int64 {
	return atomic.LoadInt64(&s.budgets[idx].Value)
}

// CheckAndSpend limits heap escape by avoiding exposing pointers to caller stack frames.
func (s *BudgetStore) CheckAndSpend(idx uint32, limit int64) bool {
	ptr := &s.budgets[idx].Value
	for {
		curr := atomic.LoadInt64(ptr)
		if curr < limit {
			return false
		}
		if atomic.CompareAndSwapInt64(ptr, curr, curr-limit) {
			return true
		}
	}
}

func (s *BudgetStore) GetBudget(id uuid.UUID) int64 {
	s.mu.RLock()
	idx, exists := s.slots[id]
	s.mu.RUnlock()
	if !exists {
		return 0
	}
	return atomic.LoadInt64(&s.budgets[idx].Value)
}

func (s *BudgetStore) SetBudget(id uuid.UUID, val int64) {
	s.mu.Lock()
	idx, exists := s.slots[id]
	if !exists {
		idx = uint32(len(s.budgets))
		s.budgets = append(s.budgets, AlignedBudget{Value: val})
		s.slots[id] = idx
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	atomic.StoreInt64(&s.budgets[idx].Value, val)
}

// CampaignAuctionRegistry organizes campaign metadata in a Structure of Arrays (SoA) layout.
type CampaignAuctionRegistry struct {
	Count         int
	CampaignIDs   []uuid.UUID
	BidFloors     []int64
	DeviceMasks   []uint8
	CategoryMasks []uint64
	GeoHashes     []uint32
	Weights       []uint32
	BudgetIndices []uint32
}

type CampaignData struct {
	ID           uuid.UUID
	BidFloor     int64
	DeviceMask   uint8
	CategoryMask uint64
	GeoHashVal   uint32
	Weight       uint32
	Budget       int64
}

// PaddedShard isolates partitions to prevent cache line bouncing during pointer swaps.
type PaddedShard struct {
	pointer atomic.Pointer[CampaignAuctionRegistry]
	_       [56]byte
}

type Registry struct {
	shards [16]PaddedShard
	store  *BudgetStore
}

func NewRegistry(store *BudgetStore) *Registry {
	r := &Registry{store: store}
	for i := 0; i < 16; i++ {
		empty := &CampaignAuctionRegistry{}
		r.shards[i].pointer.Store(empty)
	}
	return r
}

func (r *Registry) LoadShard(idx uint32) *CampaignAuctionRegistry {
	return r.shards[idx&15].pointer.Load()
}

func (r *Registry) Store() *BudgetStore {
	return r.store
}

func (r *Registry) UpdateCampaigns(campaigns []CampaignData) {
	var grouped [16][]CampaignData
	for _, c := range campaigns {
		shardIdx := c.GeoHashVal & 15
		grouped[shardIdx] = append(grouped[shardIdx], c)
	}

	for shardIdx := 0; shardIdx < 16; shardIdx++ {
		shardCampaigns := grouped[shardIdx]
		n := len(shardCampaigns)

		newReg := &CampaignAuctionRegistry{
			Count:         n,
			CampaignIDs:   make([]uuid.UUID, n),
			BidFloors:     make([]int64, n),
			DeviceMasks:   make([]uint8, n),
			CategoryMasks: make([]uint64, n),
			GeoHashes:     make([]uint32, n),
			Weights:       make([]uint32, n),
			BudgetIndices: make([]uint32, n),
		}

		for i, c := range shardCampaigns {
			newReg.CampaignIDs[i] = c.ID
			newReg.BidFloors[i] = c.BidFloor
			newReg.DeviceMasks[i] = c.DeviceMask
			newReg.CategoryMasks[i] = c.CategoryMask
			newReg.GeoHashes[i] = c.GeoHashVal
			newReg.Weights[i] = c.Weight
			newReg.BudgetIndices[i] = r.store.GetOrAllocateSlot(c.ID, c.Budget)
		}

		r.shards[shardIdx].pointer.Store(newReg)
	}
}
