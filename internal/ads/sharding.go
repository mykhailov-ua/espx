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
