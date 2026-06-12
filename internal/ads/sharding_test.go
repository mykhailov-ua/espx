package ads

import (
	"hash/crc32"
	"testing"

	"github.com/google/uuid"
)

func TestCRC32Castagnoli(t *testing.T) {
	table := crc32.MakeTable(crc32.Castagnoli)
	ids := []uuid.UUID{uuid.Nil, uuid.New(), uuid.New(), uuid.New()}
	for _, id := range ids {
		want := crc32.Checksum(id[:], table)
		if got := crc32Castagnoli(&id); got != want {
			t.Fatalf("id=%s: crc32Castagnoli=%08x want %08x", id, got, want)
		}
	}
}

func TestJumpHashSharder_GetShard(t *testing.T) {
	numShards := 10
	sharder := NewJumpHashSharder(numShards)

	id := uuid.New()
	shard1 := sharder.GetShard(id)
	shard2 := sharder.GetShard(id)
	if shard1 != shard2 {
		t.Errorf("expected deterministic shard, got %d and %d", shard1, shard2)
	}

	for i := 0; i < 1000; i++ {
		shard := sharder.GetShard(uuid.New())
		if shard < 0 || shard >= numShards {
			t.Errorf("shard %d out of range [0, %d)", shard, numShards)
		}
	}

	counts := make(map[int]int)
	total := 10000
	for i := 0; i < total; i++ {
		shard := sharder.GetShard(uuid.New())
		counts[shard]++
	}

	for shard, count := range counts {
		expected := total / numShards
		tolerance := expected / 2
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

func TestStaticSlotSharder_GetShard(t *testing.T) {
	numShards := 10
	sharder := NewStaticSlotSharder(numShards)

	id := uuid.New()
	shard1 := sharder.GetShard(id)
	shard2 := sharder.GetShard(id)
	if shard1 != shard2 {
		t.Errorf("expected deterministic shard, got %d and %d", shard1, shard2)
	}

	for i := 0; i < 1000; i++ {
		shard := sharder.GetShard(uuid.New())
		if shard < 0 || shard >= numShards {
			t.Errorf("shard %d out of range [0, %d)", shard, numShards)
		}
	}
}

func BenchmarkStaticSlotSharder_10(b *testing.B) {
	s := NewStaticSlotSharder(10)
	id := uuid.New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.GetShard(id)
	}
}

func BenchmarkStaticSlotSharder_1024(b *testing.B) {
	s := NewStaticSlotSharder(1024)
	id := uuid.New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.GetShard(id)
	}
}
