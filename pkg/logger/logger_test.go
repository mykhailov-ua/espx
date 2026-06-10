package logger

import (
	"encoding/binary"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func TestComputePersistQueueDepth(t *testing.T) {
	if got := ComputePersistQueueDepth(Config{FlushBufferSize: 256 * 1024}); got < minPersistQueueDepth {
		t.Fatalf("auto depth=%d want >= %d", got, minPersistQueueDepth)
	}
	if got := ComputePersistQueueDepth(Config{PersistQueueDepth: 128}); got != 128 {
		t.Fatalf("explicit depth=%d want 128", got)
	}
	if got := ComputePersistQueueDepth(Config{PersistQueueDepth: 99999}); got != maxPersistQueueDepth {
		t.Fatalf("capped depth=%d want %d", got, maxPersistQueueDepth)
	}
}

func TestSendBufferEnqueueTimeout(t *testing.T) {
	l := &Logger{
		cfg: Config{
			FlushBufferSize:       4096,
			PersistEnqueueTimeout: 2 * time.Millisecond,
		},
		persistCh:       make(chan *AlignedBuffer, 1),
		persistQueueCap: 1,
		closeChan:       make(chan struct{}),
	}
	l.persistCh <- NewAlignedBuffer(4096)

	buf := NewAlignedBuffer(4096)
	buf.offset = 512
	l.sendBuffer(buf, false)

	if l.persistQueueDrops.Load() != 1 {
		t.Fatalf("drops=%d want 1", l.persistQueueDrops.Load())
	}
	if l.persistQueueDropBytes.Load() != 512 {
		t.Fatalf("drop bytes=%d want 512", l.persistQueueDropBytes.Load())
	}
}

func TestLoggerZeroAlloc(t *testing.T) {
	cfg := Config{
		LogDir:           t.TempDir(),
		FlushBufferSize:  4096,
		RotateSize:       1024 * 1024,
		RotateInterval:   time.Hour,
		DiskLatencyLimit: time.Second,
	}
	l := NewLogger(cfg, 1)
	defer l.Close()
	data := []byte("high-performance zero-allocation production telemetry test log line")
	l.WriteToShard(0, 1, data)
	allocs := testing.AllocsPerRun(1000, func() {
		ok := l.WriteToShard(0, 1, data)
		if !ok {
			t.Fatal("write failed")
		}
	})
	if allocs > 0 {
		t.Errorf("Hot-path write produced %f allocations, expected 0", allocs)
	}
}

func TestLogShardMPSCConcurrent(t *testing.T) {
	const (
		producers = 8
		perProd   = 2000
	)
	s := NewLogShard()
	line := []byte("mpsc concurrent log line payload")
	var wg sync.WaitGroup
	wg.Add(producers)
	for p := 0; p < producers; p++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perProd; i++ {
				if !s.Write(1, line) {
					t.Error("write failed under load")
					return
				}
			}
		}()
	}
	wg.Wait()

	want := uint64(producers * perProd)
	if got := atomic.LoadUint64(&s.writeCursor); got != want {
		t.Fatalf("writeCursor=%d want %d", got, want)
	}
	if got := atomic.LoadUint64(&s.allocCursor); got != want {
		t.Fatalf("allocCursor=%d want %d", got, want)
	}

	buf := NewAlignedBuffer(4 * 1024 * 1024)
	l := &Logger{
		cfg:    Config{FlushBufferSize: 4 * 1024 * 1024},
		shards: []*LogShard{s},
	}
	buf, _ = l.drainShards(buf)
	if buf.offset == 0 {
		t.Fatal("drain produced empty buffer")
	}
}

func TestLogShardMPSCUniqueLines(t *testing.T) {
	const producers = 16
	s := NewLogShard()
	var wg sync.WaitGroup
	wg.Add(producers)
	for p := 0; p < producers; p++ {
		p := p
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				msg := []byte("p=" + strconv.Itoa(p) + " i=" + strconv.Itoa(i))
				if !s.Write(1, msg) {
					t.Error("write failed")
					return
				}
			}
		}()
	}
	wg.Wait()

	buf := NewAlignedBuffer(8 * 1024 * 1024)
	l := &Logger{
		cfg:    Config{FlushBufferSize: 8 * 1024 * 1024},
		shards: []*LogShard{s},
	}
	buf, _ = l.drainShards(buf)
	data := buf.Bytes()
	lines := make(map[string]bool)
	for len(data) > 0 {
		if len(data) < 4 {
			t.Fatal("malformed length prefix")
		}
		length := binary.BigEndian.Uint32(data[:4])
		data = data[4:]
		if uint32(len(data)) < length {
			t.Fatal("truncated payload")
		}
		lines[string(data[:length])] = true
		data = data[length:]
	}
	for p := 0; p < producers; p++ {
		for i := 0; i < 500; i++ {
			needle := "p=" + strconv.Itoa(p) + " i=" + strconv.Itoa(i)
			if !lines[needle] {
				t.Fatalf("missing line %q in drained output", needle)
			}
		}
	}
}

func TestLoggerRingBufferOverflow(t *testing.T) {
	s := NewLogShard()
	data := []byte("overflow testing line")
	for i := 0; i < ringUsable; i++ {
		ok := s.Write(0, data)
		if !ok {
			t.Fatalf("early drop at %d", i)
		}
	}
	ok := s.Write(0, data)
	if ok {
		t.Fatal("expected drop on saturation")
	}
}

func TestLoggerDiskDegradationEmergency(t *testing.T) {
	cfg := Config{
		LogDir:           t.TempDir(),
		FlushBufferSize:  4096,
		RotateSize:       1024 * 1024,
		RotateInterval:   time.Hour,
		DiskLatencyLimit: time.Second,
	}
	l := NewLogger(cfg, 1)
	defer l.Close()
	l.diskDegraded.Store(1)
	data := []byte("telemetry log entry")
	ok := l.WriteToShard(0, 0, data)
	if !ok {
		t.Fatal("write failed")
	}
	buf := l.getBuffer()
	buf, _ = l.drainShards(buf)
	dropped := l.loadSheddingEvents.Load()
	if dropped != 1 {
		t.Errorf("expected 1 dropped log, got %d", dropped)
	}
	if buf.offset > 0 {
		t.Error("expected empty buffer")
	}
}

func TestLoggerRotation(t *testing.T) {
	logDir := t.TempDir()
	cfg := Config{
		LogDir:           logDir,
		FlushBufferSize:  4096,
		RotateSize:       10,
		RotateInterval:   time.Hour,
		DiskLatencyLimit: time.Second,
	}
	l := NewLogger(cfg, 1)
	defer l.Close()
	data := []byte("some data to force rotation")
	l.WriteToShard(0, 1, data)
	time.Sleep(100 * time.Millisecond)
	l.WriteToShard(0, 1, data)
	time.Sleep(100 * time.Millisecond)
	pattern := filepath.Join(logDir, "segment_*.log.zst.ready")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) == 0 {
		t.Fatal("expected rotated segment file ending with .ready")
	}
}

func TestLogPayloadSize(t *testing.T) {
	var p LogPayload
	size := unsafe.Sizeof(p)
	if size != 512 {
		t.Errorf("expected unsafe.Sizeof(LogPayload) to be 512, got %d", size)
	}
}

func TestLoggerEncryptionDecryption(t *testing.T) {
	t.Setenv("LOG_ENCRYPTION_KEY", "test-super-secret-passphrase")

	logDir := t.TempDir()
	cfg := Config{
		LogDir:                logDir,
		FlushBufferSize:       4096,
		RotateSize:            1024 * 1024,
		RotateInterval:        time.Hour,
		DiskLatencyLimit:      time.Second,
		PersistEnqueueTimeout: time.Second,
	}

	l := NewLogger(cfg, 1)

	lines := [][]byte{
		[]byte("first high-performance encrypted log line"),
		[]byte("second high-performance encrypted log line"),
		[]byte("third high-performance encrypted log line"),
	}

	for _, line := range lines {
		ok := l.WriteToShard(0, 1, line)
		if !ok {
			t.Fatal("write to shard failed")
		}
	}

	l.Close()

	key := DeriveKey("test-super-secret-passphrase")

	activePath := filepath.Join(logDir, "active.log")
	decryptedBytes, err := DecryptSegment(activePath, key)
	if err != nil {
		t.Fatalf("failed to decrypt segment: %v", err)
	}

	data := decryptedBytes
	for _, expectedLine := range lines {
		if len(data) < 4 {
			t.Fatalf("decrypted data too short, expected length prefix")
		}
		length := binary.BigEndian.Uint32(data[:4])
		data = data[4:]
		if uint32(len(data)) < length {
			t.Fatalf("decrypted data too short, expected payload of length %d", length)
		}
		actualLine := data[:length]
		data = data[length:]

		if string(actualLine) != string(expectedLine) {
			t.Errorf("mismatch: got %q, want %q", actualLine, expectedLine)
		}
	}

	if len(data) > 0 {
		t.Errorf("unexpected trailing data in decrypted stream: %d bytes", len(data))
	}
}
