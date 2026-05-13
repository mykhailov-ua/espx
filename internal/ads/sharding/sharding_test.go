package sharding

import (
	"testing"

	"github.com/google/uuid"
)

func TestJumpHashSharder_GetShard(t *testing.T) {
	numShards := 10
	sharder := NewJumpHashSharder(numShards)

	// Test determinism
	id := uuid.New()
	shard1 := sharder.GetShard(id)
	shard2 := sharder.GetShard(id)
	if shard1 != shard2 {
		t.Errorf("expected deterministic shard, got %d and %d", shard1, shard2)
	}

	// Test range
	for i := 0; i < 1000; i++ {
		shard := sharder.GetShard(uuid.New())
		if shard < 0 || shard >= numShards {
			t.Errorf("shard %d out of range [0, %d)", shard, numShards)
		}
	}

	// Test distribution (rough)
	counts := make(map[int]int)
	total := 10000
	for i := 0; i < total; i++ {
		shard := sharder.GetShard(uuid.New())
		counts[shard]++
	}

	for shard, count := range counts {
		expected := total / numShards
		tolerance := expected / 2 // Very loose tolerance for 10k samples
		if count < expected-tolerance || count > expected+tolerance {
			t.Errorf("shard %d has poor distribution: %d events (expected ~%d)", shard, count, expected)
		}
	}
}

func TestJumpHashSharder_Consistency(t *testing.T) {
	id := uuid.New()

	s5 := NewJumpHashSharder(5)
	shard5 := s5.GetShard(id)

	s6 := NewJumpHashSharder(6)
	shard6 := s6.GetShard(id)

	// In consistent hashing, if we move from 5 to 6 shards,
	// the key should either stay in its original shard or move to the new shard (5).
	if shard6 != shard5 && shard6 != 5 {
		t.Errorf("non-consistent move (up): shard moved from %d (5 shards) to %d (6 shards)", shard5, shard6)
	}
}

func TestJumpHashSharder_ScaleDown(t *testing.T) {
	id := uuid.New()

	s6 := NewJumpHashSharder(6)
	shard6 := s6.GetShard(id)

	s5 := NewJumpHashSharder(5)
	shard5 := s5.GetShard(id)

	// If the key was on shard 5, it MUST move to one of 0-4.
	// If it was on 0-4, it MUST stay there.
	if shard6 < 5 {
		if shard5 != shard6 {
			t.Errorf("non-consistent move (down): shard moved from %d to %d even though it was within range", shard6, shard5)
		}
	} else {
		if shard5 >= 5 {
			t.Errorf("failed to re-distribute: shard was %d, stayed %d after scale down to 5", shard6, shard5)
		}
	}
}

func TestJumpHashSharder_BoundaryValues(t *testing.T) {
	tests := []struct {
		numBuckets int
		expected   int
	}{
		{0, 0},
		{-1, 0},
		{1, 0},
	}

	for _, tt := range tests {
		s := NewJumpHashSharder(tt.numBuckets)
		shard := s.GetShard(uuid.New())
		if shard != tt.expected {
			t.Errorf("expected shard %d for numBuckets %d, got %d", tt.expected, tt.numBuckets, shard)
		}
	}
}

func TestJumpHashSharder_NilUUID(t *testing.T) {
	s := NewJumpHashSharder(10)
	shard1 := s.GetShard(uuid.Nil)
	shard2 := s.GetShard(uuid.Nil)

	if shard1 != shard2 {
		t.Error("nil UUID should be deterministic")
	}
}

func BenchmarkJumpHashSharder_10(b *testing.B) {
	s := NewJumpHashSharder(10)
	id := uuid.New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.GetShard(id)
	}
}

func BenchmarkJumpHashSharder_1024(b *testing.B) {
	s := NewJumpHashSharder(1024)
	id := uuid.New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.GetShard(id)
	}
}
