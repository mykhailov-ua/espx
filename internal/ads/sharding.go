package ads

import (
	"hash/crc32"

	"github.com/google/uuid"
)

type Sharder interface {
	GetShard(id uuid.UUID) int
}

type JumpHashSharder struct {
	numBuckets int
}

func NewJumpHashSharder(numBuckets int) *JumpHashSharder {
	if numBuckets <= 0 {
		numBuckets = 1
	}
	return &JumpHashSharder{numBuckets: numBuckets}
}

var crc32Table = crc32.IEEETable

// crc32IEEE computes the IEEE CRC32 checksum for a 16-byte UUID array
// with zero allocations and no heap escapes.
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
