// Package ads implements UUID-to-shard mapping for Redis key routing. Two strategies
// are provided and are selected at runtime based on topology requirements:
//
//   - StaticSlotSharder: pre-computes a 1 024-slot lookup table at construction time.
//     GetShard is a single CRC32 + bitmask + array index, O(1) with no divisions.
//     Preferred for static shard counts known at deploy time.
//
//   - JumpHashSharder: applies Google's jump consistent hash (Lamping & Veach, 2014)
//     on the CRC32 of the UUID. Output distribution is uniform with minimal remapping
//     on shard-count changes. Use when online resharding is required.
//
// The shared crc32IEEE function inlines the CRC32/IEEE checksum over the 16-byte UUID
// without allocating a []byte slice. The table reference is package-level to ensure
// the compiler keeps it in L1 via a single global pointer load per call site.
package ads

import (
	"hash/crc32"

	"github.com/google/uuid"
)

// Sharder maps a campaign UUID to a Redis shard index in the range [0, numBuckets).
// Implementations must be safe for concurrent use.
type Sharder interface {
	GetShard(id uuid.UUID) int
}

// JumpHashSharder applies the jump consistent hash algorithm to the CRC32 of the
// UUID. O(log n) with n = numBuckets; produces minimal key movement when numBuckets
// increases by one. Safe for concurrent use (no shared mutable state).
type JumpHashSharder struct {
	numBuckets int
}

// StaticSlotSharder uses a pre-computed 1 024-entry lookup table. The CRC32 of the
// UUID is masked to 10 bits to select a slot; each slot holds a pre-assigned bucket
// index. This avoids a division on every call at the cost of 2 KiB of memory.
type StaticSlotSharder struct {
	slots [1024]uint16
}

func NewStaticSlotSharder(numBuckets int) *StaticSlotSharder {
	if numBuckets <= 0 {
		numBuckets = 1
	}
	s := &StaticSlotSharder{}
	for i := 0; i < 1024; i++ {
		s.slots[i] = uint16(i % numBuckets)
	}
	return s
}

func (s *StaticSlotSharder) GetShard(id uuid.UUID) int {
	key := crc32IEEE(id)
	slot := key & 1023
	return int(s.slots[slot])
}

func NewJumpHashSharder(numBuckets int) *JumpHashSharder {
	if numBuckets <= 0 {
		numBuckets = 1
	}
	return &JumpHashSharder{numBuckets: numBuckets}
}

var crc32Table = crc32.IEEETable

func crc32IEEE(data uuid.UUID) uint32 {
	var crc uint32 = 0xffffffff
	for i := 0; i < 16; i++ {
		crc = crc32Table[byte(crc)^data[i]] ^ (crc >> 8)
	}
	return ^crc
}

func (s *JumpHashSharder) GetShard(id uuid.UUID) int {
	if s.numBuckets <= 1 {
		return 0
	}

	key := uint64(crc32IEEE(id))

	return int(jumpHash(key, int32(s.numBuckets)))
}

func jumpHash(key uint64, numBuckets int32) int32 {
	var b int64 = -1
	var j int64
	for j < int64(numBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(1<<31) / float64((key>>33)+1)))
	}
	return int32(b)
}
