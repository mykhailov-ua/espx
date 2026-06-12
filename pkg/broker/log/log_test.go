package log

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestSegmentWriteAndRead(t *testing.T) {
	dir, err := os.MkdirTemp("", "segment-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	seg, err := NewSegment(dir, 0, 1024*1024, 4096, true)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	payload := []byte("hello world")
	pos, err := seg.Write(100, payload)
	if err != nil {
		t.Fatal(err)
	}

	if pos != 0 {
		t.Errorf("expected pos 0, got %d", pos)
	}

	targetPos, msgCount, totalMsgBytes, err := seg.LocateMessages(0, 100, 1024)
	if err != nil {
		t.Fatal(err)
	}

	if targetPos != 0 {
		t.Errorf("expected targetPos 0, got %d", targetPos)
	}
	if msgCount != 1 {
		t.Errorf("expected msgCount 1, got %d", msgCount)
	}
	expectedLen := uint32(12 + len(payload))
	if totalMsgBytes != expectedLen {
		t.Errorf("expected totalMsgBytes %d, got %d", expectedLen, totalMsgBytes)
	}
}

func BenchmarkSegmentWrite(b *testing.B) {
	dir, err := os.MkdirTemp("", "segment-bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	maxSize := int64(b.N) * 100
	if maxSize < 1024*1024 {
		maxSize = 1024 * 1024
	}
	seg, err := NewSegment(dir, 0, maxSize, 4096, true)
	if err != nil {
		b.Fatal(err)
	}
	defer seg.Close()

	payload := []byte("hello world")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := seg.Write(uint64(i), payload)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestSegmentRecoveryWithMalformedTail(t *testing.T) {
	dir, err := os.MkdirTemp("", "segment-recovery-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	seg, err := NewSegment(dir, 0, 1024*1024, 4096, true)
	if err != nil {
		t.Fatal(err)
	}

	p1 := []byte("first valid record")
	pos1, err := seg.Write(100, p1)
	if err != nil {
		t.Fatal(err)
	}

	p2 := []byte("second valid record")
	pos2, err := seg.Write(101, p2)
	if err != nil {
		t.Fatal(err)
	}

	err = seg.Close()
	if err != nil {
		t.Fatal(err)
	}

	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", 0))
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.Write([]byte{0, 0, 0, 100})
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	seg2, err := NewSegment(dir, 0, 1024*1024, 4096, true)
	if err != nil {
		t.Fatal(err)
	}
	defer seg2.Close()

	nextOffset, err := seg2.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
	t.Logf("After recovery: nextOffset=%d, logSize=%d, maxSegSize=%d", nextOffset, seg2.logSize, seg2.maxSegSize)

	p3 := []byte("third valid record")
	pos3, err := seg2.Write(nextOffset, p3)
	if err != nil {
		t.Fatalf("Write failed: %v (logSize=%d, maxSegSize=%d)", err, seg2.logSize, seg2.maxSegSize)
	}

	if pos1 != 0 {
		t.Errorf("expected pos1 to be 0, got %d", pos1)
	}
	expectedPos2 := int64(12 + len(p1))
	if pos2 != expectedPos2 {
		t.Errorf("expected pos2 to be %d, got %d", expectedPos2, pos2)
	}
	expectedPos3 := expectedPos2 + int64(12+len(p2))
	if pos3 != expectedPos3 {
		t.Errorf("expected pos3 to be %d, got %d", expectedPos3, pos3)
	}

	targetPos, msgCount, totalMsgBytes, err := seg2.LocateMessages(0, 100, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	if targetPos != 0 {
		t.Errorf("expected targetPos to be 0, got %d", targetPos)
	}
	if msgCount != 3 {
		t.Errorf("expected msgCount to be 3, got %d", msgCount)
	}
	expectedTotalBytes := uint32(12 + len(p1) + 12 + len(p2) + 12 + len(p3))
	if totalMsgBytes != expectedTotalBytes {
		t.Errorf("expected totalMsgBytes to be %d, got %d", expectedTotalBytes, totalMsgBytes)
	}
}
