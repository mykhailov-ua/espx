package rtb

import (
	"github.com/google/uuid"
)

// BidRequest is ordered descending by field size to eliminate compiler padding.
type BidRequest struct {
	CategoryMask uint64
	MinBid       int64
	GeoHash      uint32
	DeviceType   uint8
}

type AuctionResult struct {
	CampaignID uuid.UUID
	Price      int64
}

func (r *Registry) RunAuction(req *BidRequest) (AuctionResult, bool) {
	if req == nil || req.MinBid < 0 {
		return AuctionResult{}, false
	}
	shardIdx := req.GeoHash & 15
	reg := r.LoadShard(shardIdx)
	if reg == nil || reg.Count == 0 {
		return AuctionResult{}, false
	}

	count := reg.Count
	campaignIDs := reg.CampaignIDs
	bidFloors := reg.BidFloors
	deviceMasks := reg.DeviceMasks
	categoryMasks := reg.CategoryMasks
	geoHashes := reg.GeoHashes
	budgetIndices := reg.BudgetIndices

	// BCE hints.
	if count > 0 {
		_ = campaignIDs[count-1]
		_ = bidFloors[count-1]
		_ = deviceMasks[count-1]
		_ = categoryMasks[count-1]
		_ = geoHashes[count-1]
		_ = budgetIndices[count-1]
	}

	var candidates [128]uint32
	matchedCount := 0

	for i := 0; i < count; i++ {
		if geoHashes[i] != req.GeoHash {
			continue
		}
		if (deviceMasks[i] & req.DeviceType) == 0 {
			continue
		}
		if (categoryMasks[i] & req.CategoryMask) == 0 {
			continue
		}
		bid := bidFloors[i]
		if bid < req.MinBid {
			continue
		}
		budgetIdx := budgetIndices[i]
		if r.store.LoadBudget(budgetIdx) < bid {
			continue
		}

		// Lazy heapification to defer heap construction overhead.
		if matchedCount < 128 {
			candidates[matchedCount] = uint32(i)
			matchedCount++
		} else {
			if matchedCount == 128 {
				for parent := 63; parent >= 0; parent-- {
					siftDown(candidates[:128], bidFloors, parent)
				}
				matchedCount++
			}
			if bid > bidFloors[candidates[0]] {
				candidates[0] = uint32(i)
				siftDown(candidates[:128], bidFloors, 0)
			}
		}
	}

	if matchedCount == 0 {
		return AuctionResult{}, false
	}

	limit := matchedCount
	if limit > 128 {
		limit = 128
	}

	var winnerIdx int = -1
	var maxBid int64 = -1
	var secondBid int64 = -1

	for _, cIdx := range candidates[:limit] {
		if int(cIdx) >= len(bidFloors) {
			continue
		}
		bVal := bidFloors[cIdx]
		if bVal > maxBid {
			secondBid = maxBid
			maxBid = bVal
			winnerIdx = int(cIdx)
		} else if bVal > secondBid {
			secondBid = bVal
		}
	}

	if winnerIdx == -1 {
		return AuctionResult{}, false
	}

	price := req.MinBid
	if secondBid != -1 && secondBid > price {
		price = secondBid
	}

	if winnerIdx >= len(budgetIndices) || winnerIdx >= len(campaignIDs) {
		return AuctionResult{}, false
	}

	winnerBudgetIdx := budgetIndices[winnerIdx]
	if !r.store.CheckAndSpend(winnerBudgetIdx, price) {
		return AuctionResult{}, false
	}

	return AuctionResult{
		CampaignID: campaignIDs[winnerIdx],
		Price:      price,
	}, true
}

func siftDown(heap []uint32, bids []int64, idx int) {
	n := len(heap)
	for {
		left := 2*idx + 1
		right := left + 1
		smallest := idx

		if left < n && bids[heap[left]] < bids[heap[smallest]] {
			smallest = left
		}
		if right < n && bids[heap[right]] < bids[heap[smallest]] {
			smallest = right
		}
		if smallest == idx {
			break
		}
		heap[idx], heap[smallest] = heap[smallest], heap[idx]
		idx = smallest
	}
}
