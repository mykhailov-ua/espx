package protocol

import (
	"bytes"
	"math/rand"
	"strconv"
	"testing"
)

// TestTopicRegistry ensures stable numeric IDs and idempotent topic registration.
func TestTopicRegistry(t *testing.T) {
	registry := NewTopicRegistry()

	id1, err := registry.Register("topic-a")
	if err != nil {
		t.Fatalf("failed to register topic-a: %v", err)
	}
	id2, err := registry.Register("topic-b")
	if err != nil {
		t.Fatalf("failed to register topic-b: %v", err)
	}

	if id1 == 0 || id2 == 0 {
		t.Fatalf("got invalid topic IDs: %d, %d", id1, id2)
	}
	if id1 == id2 {
		t.Fatalf("expected different IDs, got %d for both", id1)
	}

	meta1, found := registry.Lookup(id1)
	if !found {
		t.Fatalf("failed to lookup topic-a by ID %d", id1)
	}
	if meta1.Name != "topic-a" {
		t.Fatalf("expected topic-a, got %s", meta1.Name)
	}

	meta2, found := registry.Lookup(id2)
	if !found {
		t.Fatalf("failed to lookup topic-b by ID %d", id2)
	}
	if meta2.Name != "topic-b" {
		t.Fatalf("expected topic-b, got %s", meta2.Name)
	}

	id1_again, err := registry.Register("topic-a")
	if err != nil {
		t.Fatalf("failed to register topic-a again: %v", err)
	}
	if id1_again != id1 {
		t.Fatalf("expected existing ID %d, got %d", id1, id1_again)
	}

	_, found = registry.Lookup(999)
	if found {
		t.Fatalf("expected lookup of unregistered ID to fail")
	}
}

// TestBatchIterator validates zero-alloc iteration over batched wire messages.
func TestBatchIterator(t *testing.T) {
	var batch []byte

	messages := []struct {
		topicID uint16
		payload []byte
	}{
		{topicID: 10, payload: []byte("hello")},
		{topicID: 20, payload: []byte("world")},
		{topicID: 30, payload: []byte("")},
		{topicID: 40, payload: []byte("zero-allocation-batching")},
	}

	for _, msg := range messages {
		batch = AppendBatchMessage(batch, msg.topicID, msg.payload)
	}

	it := NewBatchIterator(batch)
	count := 0
	for it.Next() {
		if count >= len(messages) {
			t.Fatalf("iterated more messages than expected")
		}
		expected := messages[count]
		if it.TopicID != expected.topicID {
			t.Errorf("msg %d: expected topic ID %d, got %d", count, expected.topicID, it.TopicID)
		}
		if !bytes.Equal(it.Payload, expected.payload) {
			t.Errorf("msg %d: expected payload %s, got %s", count, string(expected.payload), string(it.Payload))
		}
		count++
	}

	if count != len(messages) {
		t.Fatalf("expected to iterate %d messages, got %d", len(messages), count)
	}
}

// TestReadFrameNewCommands covers batch produce and topic registration decoding.
func TestReadFrameNewCommands(t *testing.T) {
	writeBuf := make([]byte, 1024)
	readBuf := make([]byte, 1024)
	lenBuf := make([]byte, 4)

	batchPayload := []byte("batch-payload-data")
	reqBuf := EncodeProduceBatchRequest(writeBuf, 42, batchPayload)

	r := bytes.NewReader(reqBuf)
	cmd, seq, payload, err := ReadFrame(r, readBuf, lenBuf)
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}
	if cmd != CmdProduceBatch {
		t.Errorf("expected CmdProduceBatch (%d), got %d", CmdProduceBatch, cmd)
	}
	if seq != 42 {
		t.Errorf("expected seq 42, got %d", seq)
	}
	if !bytes.Equal(payload, batchPayload) {
		t.Errorf("expected payload %s, got %s", string(batchPayload), string(payload))
	}

	topicName := "test-topic-registration"
	reqBuf2 := EncodeRegisterTopicRequest(writeBuf, 100, topicName)

	r2 := bytes.NewReader(reqBuf2)
	cmd, seq, payload, err = ReadFrame(r2, readBuf, lenBuf)
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}
	if cmd != CmdRegisterTopic {
		t.Errorf("expected CmdRegisterTopic (%d), got %d", CmdRegisterTopic, cmd)
	}
	if seq != 100 {
		t.Errorf("expected seq 100, got %d", seq)
	}

	decodedTopic, err := DecodeRegisterTopicRequest(payload)
	if err != nil {
		t.Fatalf("DecodeRegisterTopicRequest failed: %v", err)
	}
	if decodedTopic != topicName {
		t.Errorf("expected topic %s, got %s", topicName, decodedTopic)
	}
}

// BenchmarkTopicRegistryLookup guards lock-free lookup performance at scale.
func BenchmarkTopicRegistryLookup(b *testing.B) {
	registry := NewTopicRegistry()
	var ids []uint16
	for i := 0; i < 1000; i++ {
		id, _ := registry.Register("topic-" + strconv.Itoa(i))
		ids = append(ids, id)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(42))
		for pb.Next() {
			id := ids[r.Intn(len(ids))]
			meta, found := registry.Lookup(id)
			if !found || meta == nil {
				b.Fatal("lookup failed")
			}
		}
	})
}

// BenchmarkBatchIterator measures batch walk cost for high-throughput produce paths.
func BenchmarkBatchIterator(b *testing.B) {
	var batch []byte
	for i := 0; i < 100; i++ {
		batch = AppendBatchMessage(batch, uint16(i%1000), []byte("message-payload-data-for-benchmarking"))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		it := NewBatchIterator(batch)
		count := 0
		for it.Next() {
			if it.TopicID == 9999 {
				b.Fatal("unexpected topic ID")
			}
			count++
		}
		if count != 100 {
			b.Fatal("incorrect message count")
		}
	}
}

// TestReadFrameChecksumVerification rejects corrupted payloads before handlers run.
func TestReadFrameChecksumVerification(t *testing.T) {
	writeBuf := make([]byte, 1024)
	readBuf := make([]byte, 1024)
	lenBuf := make([]byte, 4)

	payload := []byte("test-payload-for-checksum")
	reqBuf := EncodeProduceBatchRequest(writeBuf, 42, payload)

	r := bytes.NewReader(reqBuf)
	cmd, seq, readPayload, err := ReadFrame(r, readBuf, lenBuf)
	if err != nil {
		t.Fatalf("expected successful ReadFrame, got error: %v", err)
	}
	if cmd != CmdProduceBatch || seq != 42 || !bytes.Equal(readPayload, payload) {
		t.Fatalf("mismatched values on successful read")
	}

	corruptedBuf := make([]byte, len(reqBuf))
	copy(corruptedBuf, reqBuf)
	corruptedBuf[len(corruptedBuf)-1] ^= 0xFF

	rCorrupt := bytes.NewReader(corruptedBuf)
	_, _, _, err = ReadFrame(rCorrupt, readBuf, lenBuf)
	if err == nil {
		t.Fatalf("expected error due to corrupted checksum, but got nil")
	}
	if err.Error() != "checksum verification failed" {
		t.Errorf("expected 'checksum verification failed' error, got: %v", err)
	}
}

// BenchmarkReadFrame tracks frame decode regression on the broker hot path.
func BenchmarkReadFrame(b *testing.B) {
	writeBuf := make([]byte, 1024)
	readBuf := make([]byte, 1024)
	lenBuf := make([]byte, 4)
	batchPayload := []byte("batch-payload-data")
	reqBuf := EncodeProduceBatchRequest(writeBuf, 42, batchPayload)

	reqBufCopy := make([]byte, len(reqBuf))
	copy(reqBufCopy, reqBuf)

	b.ResetTimer()
	b.ReportAllocs()

	r := bytes.NewReader(reqBufCopy)
	for i := 0; i < b.N; i++ {
		r.Reset(reqBufCopy)
		cmd, seq, _, err := ReadFrame(r, readBuf, lenBuf)
		if err != nil {
			b.Fatal(err)
		}
		if cmd != CmdProduceBatch || seq != 42 {
			b.Fatal("invalid frame read")
		}
	}
}

// BenchmarkReadFrameSizes measures decode cost across typical batch payload sizes.
func BenchmarkReadFrameSizes(b *testing.B) {
	sizes := []int{0, 64, 512, 4096, 16384}
	for _, size := range sizes {
		b.Run("PayloadSize_"+strconv.Itoa(size), func(b *testing.B) {
			writeBuf := make([]byte, size+100)
			readBuf := make([]byte, size+100)
			lenBuf := make([]byte, 4)
			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte(i)
			}
			reqBuf := EncodeProduceBatchRequest(writeBuf, 42, payload)

			reqBufCopy := make([]byte, len(reqBuf))
			copy(reqBufCopy, reqBuf)

			b.ResetTimer()
			b.ReportAllocs()

			r := bytes.NewReader(reqBufCopy)
			for i := 0; i < b.N; i++ {
				r.Reset(reqBufCopy)
				cmd, seq, _, err := ReadFrame(r, readBuf, lenBuf)
				if err != nil {
					b.Fatal(err)
				}
				if cmd != CmdProduceBatch || seq != 42 {
					b.Fatal("invalid frame read")
				}
			}
		})
	}
}
