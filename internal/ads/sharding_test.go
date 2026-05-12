package ads

import (
	"testing"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestShardingConsistency(t *testing.T) {
	// Initializes 6 unique mock Redis clients to simulate a sharded cluster environment.
	rdbs := make([]redis.UniversalClient, 6)
	for i := 0; i < 6; i++ {
		rdbs[i] = &redis.Client{} // Use distinct objects
	}
	filter := &UnifiedFilter{
		rdbs: rdbs,
	}

	campaignID := uuid.New()

	// Validates sharding determinism: ensures the same CampaignID consistently maps to the same shard.
	shard1 := filter.getRDB(campaignID)
	for i := 0; i < 100; i++ {
		shardN := filter.getRDB(campaignID)
		assert.Equal(t, shard1, shardN, "Sharding must be deterministic")
	}

	// Validates load distribution: ensures CampaignIDs are evenly spread across the available shard cluster.
	shardCounts := make(map[redis.UniversalClient]int)
	numCampaigns := 10000
	for i := 0; i < numCampaigns; i++ {
		id := uuid.New()
		shard := filter.getRDB(id)
		shardCounts[shard]++
	}

	assert.Equal(t, 6, len(shardCounts), "All 6 shards should be utilized")
	
	// Computes expected average load and validates that per-shard skewness remains within the defined 20% tolerance.
	avg := numCampaigns / 6
	tolerance := 0.2 // 20% tolerance
	for _, count := range shardCounts {
		assert.InDelta(t, avg, count, float64(avg)*tolerance, "Shard distribution should be relatively even")
	}
}

func TestShardingSingleNode(t *testing.T) {
	rdbs := make([]redis.UniversalClient, 1)
	filter := &UnifiedFilter{
		rdbs: rdbs,
	}

	campaignID := uuid.New()
	shard := filter.getRDB(campaignID)
	assert.Equal(t, rdbs[0], shard, "Single node sharding should always return the only node")
}
