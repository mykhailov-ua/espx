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

	if count > len(campaignIDs) || count > len(bidFloors) || count > len(deviceMasks) ||
		count > len(categoryMasks) || count > len(geoHashes) || count > len(budgetIndices) {
		return AuctionResult{}, false
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

		if matchedCount < 128 {
			candidates[matchedCount] = uint32(i)
			matchedCount++
		} else {
			if matchedCount == 128 {
				for parent := 63; parent >= 0; parent-- {
					siftDown128(&candidates, bidFloors, parent)
				}
				matchedCount++
			}
			if bid > bidFloors[candidates[0]] {
				candidates[0] = uint32(i)
				siftDown128(&candidates, bidFloors, 0)
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

	_ = bidFloors[len(bidFloors)-1]

	for i := 0; i < limit; i++ {
		cIdx := candidates[i]
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

func siftDown128(heap *[128]uint32, bids []int64, idx int) {
	const n = 128
	for {
		left := (idx << 1) + 1
		right := left + 1
		if left >= n {
			break
		}
		smallest := idx
		smallestCand := heap[smallest]
		leftCand := heap[left]

		if int(smallestCand) >= len(bids) || int(leftCand) >= len(bids) {
			break
		}

		if bids[leftCand] < bids[smallestCand] {
			smallest = left
			smallestCand = leftCand
		}
		if right < n {
			rightCand := heap[right]
			if int(rightCand) >= len(bids) {
				break
			}
			if bids[rightCand] < bids[smallestCand] {
				smallest = right
			}
		}
		if smallest == idx {
			break
		}
		heap[idx], heap[smallest] = heap[smallest], heap[idx]
		idx = smallest
	}
}
