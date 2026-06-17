// Package protocol defines the internal broker wire format shared by client and server.
package protocol

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	CmdProduce           uint16 = 1
	CmdFetch             uint16 = 2
	CmdProduceBatch      uint16 = 3
	CmdRegisterTopic     uint16 = 4
	CmdProduceResp       uint16 = 101
	CmdFetchResp         uint16 = 102
	CmdProduceBatchResp  uint16 = 103
	CmdRegisterTopicResp uint16 = 104
)

type TopicMetadata struct {
	ID   uint16
	Name string
}

// TopicRegistry assigns compact numeric IDs so batch produce frames stay small on the wire.
type TopicRegistry struct {
	mu     sync.Mutex
	topics [65536]unsafe.Pointer
	byName map[string]uint16
	nextID uint32
}

func NewTopicRegistry() *TopicRegistry {
	return &TopicRegistry{
		byName: make(map[string]uint16),
	}
}

func (r *TopicRegistry) Lookup(id uint16) (*TopicMetadata, bool) {
	ptr := atomic.LoadPointer(&r.topics[id])
	if ptr == nil {
		return nil, false
	}
	return (*TopicMetadata)(ptr), true
}

func (r *TopicRegistry) Register(name string) (uint16, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if id, exists := r.byName[name]; exists {
		return id, nil
	}

	next := atomic.AddUint32(&r.nextID, 1)
	if next > 65535 {
		return 0, errors.New("topic registry limit reached (max 65535 topics)")
	}

	id := uint16(next)
	meta := &TopicMetadata{
		ID:   id,
		Name: name,
	}

	r.byName[name] = id
	atomic.StorePointer(&r.topics[id], unsafe.Pointer(meta))

	return id, nil
}

type BatchMsgHeader struct {
	TopicID    uint16
	_          uint16
	PayloadLen uint32
}

// BatchIterator walks batch payloads without per-message heap allocations.
type BatchIterator struct {
	ptr     unsafe.Pointer
	end     unsafe.Pointer
	TopicID uint16
	Payload []byte
}

func NewBatchIterator(payload []byte) BatchIterator {
	if len(payload) == 0 {
		return BatchIterator{}
	}
	start := unsafe.Pointer(&payload[0])
	return BatchIterator{
		ptr: start,
		end: unsafe.Pointer(uintptr(start) + uintptr(len(payload))),
	}
}

func (it *BatchIterator) Next() bool {
	if it.ptr == nil || uintptr(it.ptr) >= uintptr(it.end) {
		return false
	}

	remaining := uintptr(it.end) - uintptr(it.ptr)
	if remaining < 8 {
		it.ptr = it.end
		return false
	}

	hdr := (*BatchMsgHeader)(it.ptr)
	totalSize := uintptr(8) + uintptr(hdr.PayloadLen)
	if remaining < totalSize {
		it.ptr = it.end
		return false
	}

	it.TopicID = hdr.TopicID
	if hdr.PayloadLen > 0 {
		it.Payload = unsafe.Slice((*byte)(unsafe.Add(it.ptr, 8)), hdr.PayloadLen)
	} else {
		it.Payload = nil
	}

	it.ptr = unsafe.Pointer(uintptr(it.ptr) + totalSize)
	return true
}

// ReadFrame validates length, CRC, and command before handlers touch payload bytes.
func ReadFrame(r io.Reader, buf []byte, lenBuf []byte) (uint16, uint64, []byte, error) {
	if _, err := io.ReadFull(r, lenBuf[:4]); err != nil {
		return 0, 0, nil, err
	}
	length := binary.BigEndian.Uint32(lenBuf[:4])

	if length < 14 {
		return 0, 0, nil, errors.New("frame length too short")
	}

	if len(buf) < int(length) {
		return 0, 0, nil, errors.New("provided buffer too small for frame payload")
	}

	readBuf := buf[:length]
	if _, err := io.ReadFull(r, readBuf); err != nil {
		return 0, 0, nil, err
	}

	cmd := binary.BigEndian.Uint16(readBuf[0:2])
	seq := binary.BigEndian.Uint64(readBuf[2:10])
	payload := readBuf[10 : length-4]

	expected := binary.BigEndian.Uint32(readBuf[length-4:])
	calculated := crc32.ChecksumIEEE(payload)
	if calculated != expected {
		return 0, 0, nil, errors.New("checksum verification failed")
	}

	switch cmd {
	case CmdProduce, CmdFetch, CmdProduceBatch, CmdRegisterTopic,
		CmdProduceResp, CmdFetchResp, CmdProduceBatchResp, CmdRegisterTopicResp:
	default:
		return 0, 0, nil, errors.New("unknown command ID")
	}

	return cmd, seq, payload, nil
}

func DecodeProduceRequest(payload []byte) (string, []byte, error) {
	if len(payload) < 2 {
		return "", nil, errors.New("malformed produce request")
	}
	topicLen := binary.BigEndian.Uint16(payload[0:2])
	if len(payload) < 2+int(topicLen) {
		return "", nil, errors.New("malformed produce request: topic length out of bounds")
	}
	topicBytes := payload[2 : 2+topicLen]
	topic := unsafeString(topicBytes)
	msgPayload := payload[2+topicLen:]
	return topic, msgPayload, nil
}

func DecodeFetchRequest(payload []byte) (string, uint64, uint32, error) {
	if len(payload) < 14 {
		return "", 0, 0, errors.New("malformed fetch request")
	}
	topicLen := binary.BigEndian.Uint16(payload[0:2])
	if len(payload) < 2+int(topicLen)+12 {
		return "", 0, 0, errors.New("malformed fetch request: topic length out of bounds")
	}
	topicBytes := payload[2 : 2+topicLen]
	topic := unsafeString(topicBytes)
	offset := binary.BigEndian.Uint64(payload[2+topicLen : 2+topicLen+8])
	maxBytes := binary.BigEndian.Uint32(payload[2+topicLen+8 : 2+topicLen+12])
	return topic, offset, maxBytes, nil
}

func EncodeProduceRequest(buf []byte, seq uint64, topic string, payload []byte) []byte {
	topicBytes := unsafeBytes(topic)
	topicLen := len(topicBytes)
	payloadLen := len(payload)
	framePayloadLen := 2 + topicLen + payloadLen
	totalLen := uint32(2 + 8 + framePayloadLen + 4)

	binary.BigEndian.PutUint32(buf[0:4], totalLen)
	binary.BigEndian.PutUint16(buf[4:6], CmdProduce)
	binary.BigEndian.PutUint64(buf[6:14], seq)
	binary.BigEndian.PutUint16(buf[14:16], uint16(topicLen))
	copy(buf[16:16+topicLen], topicBytes)
	copy(buf[16+topicLen:16+topicLen+payloadLen], payload)

	payloadSlice := buf[14 : 14+framePayloadLen]
	checksum := crc32.ChecksumIEEE(payloadSlice)
	binary.BigEndian.PutUint32(buf[14+framePayloadLen:14+framePayloadLen+4], checksum)

	return buf[:4+2+8+framePayloadLen+4]
}

func EncodeFetchRequest(buf []byte, seq uint64, topic string, startOffset uint64, maxBytes uint32) []byte {
	topicBytes := unsafeBytes(topic)
	topicLen := len(topicBytes)
	framePayloadLen := 2 + topicLen + 8 + 4
	totalLen := uint32(2 + 8 + framePayloadLen + 4)

	binary.BigEndian.PutUint32(buf[0:4], totalLen)
	binary.BigEndian.PutUint16(buf[4:6], CmdFetch)
	binary.BigEndian.PutUint64(buf[6:14], seq)
	binary.BigEndian.PutUint16(buf[14:16], uint16(topicLen))
	copy(buf[16:16+topicLen], topicBytes)
	binary.BigEndian.PutUint64(buf[16+topicLen:16+topicLen+8], startOffset)
	binary.BigEndian.PutUint32(buf[16+topicLen+8:16+topicLen+12], maxBytes)

	payloadSlice := buf[14 : 14+framePayloadLen]
	checksum := crc32.ChecksumIEEE(payloadSlice)
	binary.BigEndian.PutUint32(buf[14+framePayloadLen:14+framePayloadLen+4], checksum)

	return buf[:4+2+8+framePayloadLen+4]
}

func EncodeProduceResponse(buf []byte, seq uint64, status byte, offset uint64) []byte {
	binary.BigEndian.PutUint32(buf[0:4], 23)
	binary.BigEndian.PutUint16(buf[4:6], CmdProduceResp)
	binary.BigEndian.PutUint64(buf[6:14], seq)
	buf[14] = status
	binary.BigEndian.PutUint64(buf[15:23], offset)

	checksum := crc32.ChecksumIEEE(buf[14:23])
	binary.BigEndian.PutUint32(buf[23:27], checksum)

	return buf[:27]
}

func EncodeFetchResponseHeader(headerBuf []byte, seq uint64, status byte, msgCount uint32, msgBytes uint32) []byte {
	totalLen := uint32(2 + 8 + 1 + 4 + msgBytes + 4)
	binary.BigEndian.PutUint32(headerBuf[0:4], totalLen)
	binary.BigEndian.PutUint16(headerBuf[4:6], CmdFetchResp)
	binary.BigEndian.PutUint64(headerBuf[6:14], seq)
	headerBuf[14] = status
	binary.BigEndian.PutUint32(headerBuf[15:19], msgCount)
	return headerBuf[:19]
}

func EncodeProduceBatchResponse(buf []byte, seq uint64, status byte, offset uint64) []byte {
	binary.BigEndian.PutUint32(buf[0:4], 23)
	binary.BigEndian.PutUint16(buf[4:6], CmdProduceBatchResp)
	binary.BigEndian.PutUint64(buf[6:14], seq)
	buf[14] = status
	binary.BigEndian.PutUint64(buf[15:23], offset)

	checksum := crc32.ChecksumIEEE(buf[14:23])
	binary.BigEndian.PutUint32(buf[23:27], checksum)

	return buf[:27]
}

func DecodeProduceBatchResponse(payload []byte) (byte, uint64, error) {
	if len(payload) < 9 {
		return 0, 0, errors.New("malformed produce batch response")
	}
	status := payload[0]
	offset := binary.BigEndian.Uint64(payload[1:9])
	return status, offset, nil
}

func EncodeRegisterTopicRequest(buf []byte, seq uint64, topic string) []byte {
	topicBytes := unsafeBytes(topic)
	topicLen := len(topicBytes)
	framePayloadLen := 2 + topicLen
	totalLen := uint32(2 + 8 + framePayloadLen + 4)

	binary.BigEndian.PutUint32(buf[0:4], totalLen)
	binary.BigEndian.PutUint16(buf[4:6], CmdRegisterTopic)
	binary.BigEndian.PutUint64(buf[6:14], seq)
	binary.BigEndian.PutUint16(buf[14:16], uint16(topicLen))
	copy(buf[16:16+topicLen], topicBytes)

	payloadSlice := buf[14 : 14+framePayloadLen]
	checksum := crc32.ChecksumIEEE(payloadSlice)
	binary.BigEndian.PutUint32(buf[14+framePayloadLen:14+framePayloadLen+4], checksum)

	return buf[:4+2+8+framePayloadLen+4]
}

func DecodeRegisterTopicRequest(payload []byte) (string, error) {
	if len(payload) < 2 {
		return "", errors.New("malformed register topic request")
	}
	topicLen := binary.BigEndian.Uint16(payload[0:2])
	if len(payload) < 2+int(topicLen) {
		return "", errors.New("malformed register topic request: topic length out of bounds")
	}
	topicBytes := payload[2 : 2+topicLen]
	return unsafeString(topicBytes), nil
}

func EncodeRegisterTopicResponse(buf []byte, seq uint64, status byte, topicID uint16) []byte {
	binary.BigEndian.PutUint32(buf[0:4], 17)
	binary.BigEndian.PutUint16(buf[4:6], CmdRegisterTopicResp)
	binary.BigEndian.PutUint64(buf[6:14], seq)
	buf[14] = status
	binary.BigEndian.PutUint16(buf[15:17], topicID)

	checksum := crc32.ChecksumIEEE(buf[14:17])
	binary.BigEndian.PutUint32(buf[17:21], checksum)

	return buf[:21]
}

func DecodeRegisterTopicResponse(payload []byte) (byte, uint16, error) {
	if len(payload) < 3 {
		return 0, 0, errors.New("malformed register topic response")
	}
	status := payload[0]
	topicID := binary.BigEndian.Uint16(payload[1:3])
	return status, topicID, nil
}

func AppendBatchMessage(buf []byte, topicID uint16, payload []byte) []byte {
	start := len(buf)
	newLen := start + 8 + len(payload)
	if cap(buf) < newLen {
		temp := make([]byte, newLen, newLen*2)
		copy(temp, buf)
		buf = temp
	} else {
		buf = buf[:newLen]
	}

	hdr := (*BatchMsgHeader)(unsafe.Pointer(&buf[start]))
	hdr.TopicID = topicID
	hdr.PayloadLen = uint32(len(payload))
	if len(payload) > 0 {
		copy(buf[start+8:newLen], payload)
	}
	return buf
}

func EncodeProduceBatchRequest(buf []byte, seq uint64, batchPayload []byte) []byte {
	framePayloadLen := len(batchPayload)
	totalLen := uint32(2 + 8 + framePayloadLen + 4)

	binary.BigEndian.PutUint32(buf[0:4], totalLen)
	binary.BigEndian.PutUint16(buf[4:6], CmdProduceBatch)
	binary.BigEndian.PutUint64(buf[6:14], seq)
	copy(buf[14:14+framePayloadLen], batchPayload)

	payloadSlice := buf[14 : 14+framePayloadLen]
	checksum := crc32.ChecksumIEEE(payloadSlice)
	binary.BigEndian.PutUint32(buf[14+framePayloadLen:14+framePayloadLen+4], checksum)

	return buf[:4+2+8+framePayloadLen+4]
}

func unsafeString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func unsafeBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
