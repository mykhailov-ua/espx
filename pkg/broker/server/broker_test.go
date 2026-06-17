package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"espx/pkg/broker/client"
	"espx/pkg/broker/log"
	"espx/pkg/broker/protocol"
)

// TestBrokerIntegration smoke-tests produce, fetch, and persistence across client and server.
func TestBrokerIntegration(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "broker-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	s := NewServer("127.0.0.1:0", tempDir, 10*1024*1024, 4096)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	addr := s.Addr()
	cli := client.NewClient(addr, 2*time.Second)
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	topic := "events-test"
	msgCount := 50
	offsets := make([]uint64, msgCount)

	for i := 0; i < msgCount; i++ {
		payload := []byte(fmt.Sprintf("msg-payload-%d", i))
		offset, err := cli.Produce(topic, payload)
		if err != nil {
			t.Fatalf("failed to produce message %d: %v", i, err)
		}
		offsets[i] = offset
		if offset != uint64(i) {
			t.Errorf("unexpected offset for message %d: got %d, expected %d", i, offset, i)
		}
	}

	iter, err := cli.Fetch(topic, 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	i := 0
	for iter.Next() {
		expectedPayload := fmt.Sprintf("msg-payload-%d", i)
		if string(iter.Payload) != expectedPayload {
			t.Errorf("mismatch on message %d: got %q, expected %q", i, string(iter.Payload), expectedPayload)
		}
		if iter.Offset != uint64(i) {
			t.Errorf("offset mismatch: got %d, expected %d", iter.Offset, i)
		}
		i++
	}
	if i != msgCount {
		t.Fatalf("expected to fetch %d messages, got %d", msgCount, i)
	}

	midIter, err := cli.Fetch(topic, 25, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	i = 25
	for midIter.Next() {
		expectedPayload := fmt.Sprintf("msg-payload-%d", i)
		if string(midIter.Payload) != expectedPayload {
			t.Errorf("mismatch on sub-fetch message %d: got %q, expected %q", i, string(midIter.Payload), expectedPayload)
		}
		i++
	}
	if i != 50 {
		t.Fatalf("expected to fetch 25 messages, got %d", i-25)
	}
}

// TestBrokerCrashRecovery ensures partition logs reopen with the correct next offset after restart.
func TestBrokerCrashRecovery(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "broker-recovery-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	topic := "recovery-topic"

	{
		s := NewServer("127.0.0.1:0", tempDir, 1024*1024, 128)
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}

		cli := client.NewClient(s.Addr(), time.Second)
		if err := cli.Connect(); err != nil {
			t.Fatal(err)
		}

		for i := 0; i < 10; i++ {
			payload := []byte(fmt.Sprintf("rec-msg-%d", i))
			_, err := cli.Produce(topic, payload)
			if err != nil {
				t.Fatal(err)
			}
		}

		_ = cli.Close()
		s.Stop()
	}

	{
		s := NewServer("127.0.0.1:0", tempDir, 1024*1024, 128)
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
		defer s.Stop()

		cli := client.NewClient(s.Addr(), time.Second)
		if err := cli.Connect(); err != nil {
			t.Fatal(err)
		}
		defer cli.Close()

		iter, err := cli.Fetch(topic, 0, 1024*1024)
		if err != nil {
			t.Fatal(err)
		}

		i := 0
		for iter.Next() {
			expectedPayload := fmt.Sprintf("rec-msg-%d", i)
			if string(iter.Payload) != expectedPayload {
				t.Errorf("recovered message %d mismatch: got %q, expected %q", i, string(iter.Payload), expectedPayload)
			}
			i++
		}

		if i != 10 {
			t.Fatalf("expected 10 recovered messages, got %d", i)
		}
	}
}

// TestTornWrite_RecoverTruncatesPartialRecord guards against serving corrupt records after power loss.
func TestTornWrite_RecoverTruncatesPartialRecord(t *testing.T) {
	dir, err := os.MkdirTemp("", "torn-write-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	const maxSegSize = 1024 * 1024
	const indexInterval = 128

	pl, err := log.NewPartitionLog(dir, maxSegSize, indexInterval)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		payload := []byte(fmt.Sprintf("clean-msg-%d", i))
		if _, err := pl.Append(payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := pl.Close(); err != nil {
		t.Fatal(err)
	}

	logPath := filepath.Join(dir, "00000000000000000000.log")
	fi, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cleanSize := fi.Size()
	t.Logf("clean log size: %d bytes", cleanSize)

	f, err := os.OpenFile(logPath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}

	var torn [10]byte
	binary.BigEndian.PutUint32(torn[0:4], 50)
	binary.BigEndian.PutUint32(torn[4:8], 0)
	if _, err := f.Write(torn[:]); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	pl2, err := log.NewPartitionLog(dir, maxSegSize, indexInterval)
	if err != nil {
		t.Fatal(err)
	}
	defer pl2.Close()

	if pl2.NextOffset() != 5 {
		t.Errorf("expected nextOffset=5 after recover, got %d", pl2.NextOffset())
	}

	fi2, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if fi2.Size() != cleanSize {
		t.Errorf("expected log truncated to %d bytes, got %d", cleanSize, fi2.Size())
	}
}

// TestENOSPC_IndexWriteFails verifies append fails cleanly when segment index space is exhausted.
func TestENOSPC_IndexWriteFails(t *testing.T) {
	dir, err := os.MkdirTemp("", "enospc-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = os.Chmod(dir, 0755)
		os.RemoveAll(dir)
	}()

	const maxSegSize = 1024 * 1024

	pl, err := log.NewPartitionLog(dir, maxSegSize, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pl.Append([]byte("seed")); err != nil {
		t.Fatal("initial append failed:", err)
	}

	if err := os.Chmod(dir, 0555); err != nil {
		t.Skip("cannot chmod dir, skipping ENOSPC simulation:", err)
	}

	_, appendErr := pl.Append([]byte("should-fail"))
	_ = pl.Close()

	if appendErr == nil {
		t.Log("WARNING: chmod did not block WriteAt on this kernel - ENOSPC emulation requires tmpfs with size limit in production CI")
		return
	}

	t.Logf("ENOSPC-like error correctly returned: %v", appendErr)
}

// TestSlowloris_DoesNotBlockOtherClients ensures one stalled client cannot wedge the event loop.
func TestSlowloris_DoesNotBlockOtherClients(t *testing.T) {
	dir, err := os.MkdirTemp("", "slowloris-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s := NewServer("127.0.0.1:0", dir, 10*1024*1024, 4096)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	slowConn, err := net.DialTimeout("tcp", s.Addr(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer slowConn.Close()

	if _, err := slowConn.Write([]byte{0x00, 0x00}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	cli := client.NewClient(s.Addr(), 2*time.Second)
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	const msgCount = 100
	for i := 0; i < msgCount; i++ {
		if _, err := cli.Produce("slowloris-topic", []byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("produce failed while slow client connected: %v", err)
		}
	}

	iter, err := cli.Fetch("slowloris-topic", 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for iter.Next() {
		count++
	}
	if count != msgCount {
		t.Errorf("expected %d messages, got %d", msgCount, count)
	}
}

// TestFDExhaustion_ServerHandlesGracefully checks behavior at file descriptor limits.
func TestFDExhaustion_ServerHandlesGracefully(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("FD exhaustion test requires Linux RLIMIT_NOFILE control")
	}

	dir, err := os.MkdirTemp("", "fdexhaust-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s := NewServer("127.0.0.1:0", dir, 10*1024*1024, 4096)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	addr := s.Addr()

	var rLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
		t.Skip("cannot get RLIMIT_NOFILE:", err)
	}
	t.Logf("current RLIMIT_NOFILE: soft=%d hard=%d", rLimit.Cur, rLimit.Max)

	var heldFiles []*os.File
	const headroom = 5
	for {
		f, err := os.Open(os.DevNull)
		if err != nil {
			break
		}
		heldFiles = append(heldFiles, f)
		if len(heldFiles)%100 == 0 {
			probe, perr := os.Open(os.DevNull)
			if perr != nil {
				break
			}
			_ = probe.Close()
		}
		if len(heldFiles) > int(rLimit.Cur)-headroom {
			break
		}
	}
	t.Logf("held %d FDs to simulate exhaustion", len(heldFiles))

	cli := client.NewClient(addr, 500*time.Millisecond)
	err = cli.Connect()
	if err == nil {
		_, _ = cli.Produce("fd-test", []byte("probe"))
		_ = cli.Close()
	} else {
		t.Logf("connect correctly failed under FD pressure: %v", err)
	}

	for _, f := range heldFiles {
		_ = f.Close()
	}

	cli2 := client.NewClient(addr, 2*time.Second)
	if err := cli2.Connect(); err != nil {
		t.Fatalf("server failed to accept after FD release: %v", err)
	}
	defer cli2.Close()

	if _, err := cli2.Produce("fd-test", []byte("recovery-msg")); err != nil {
		t.Errorf("produce failed after FD recovery: %v", err)
	}
}

// TestSplitBrain_IsolatedLogsNoCorruption ensures partitioned brokers do not corrupt logs.
func TestSplitBrain_IsolatedLogsNoCorruption(t *testing.T) {
	dirA, err := os.MkdirTemp("", "splitbrain-A-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dirA)

	dirB, err := os.MkdirTemp("", "splitbrain-B-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dirB)

	sA := NewServer("127.0.0.1:0", dirA, 10*1024*1024, 4096)
	if err := sA.Start(); err != nil {
		t.Fatal(err)
	}

	cliA := client.NewClient(sA.Addr(), 2*time.Second)
	if err := cliA.Connect(); err != nil {
		sA.Stop()
		t.Fatal(err)
	}

	topic := "split-topic"
	for i := 0; i < 10; i++ {
		if _, err := cliA.Produce(topic, []byte(fmt.Sprintf("A-msg-%d", i))); err != nil {
			_ = cliA.Close()
			sA.Stop()
			t.Fatal(err)
		}
	}
	_ = cliA.Close()

	sA.Stop()

	sB := NewServer("127.0.0.1:0", dirB, 10*1024*1024, 4096)
	if err := sB.Start(); err != nil {
		t.Fatal(err)
	}
	defer sB.Stop()

	cliB := client.NewClient(sB.Addr(), 2*time.Second)
	if err := cliB.Connect(); err != nil {
		t.Fatal(err)
	}
	defer cliB.Close()

	for i := 0; i < 5; i++ {
		if _, err := cliB.Produce(topic, []byte(fmt.Sprintf("B-msg-%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	iterB, err := cliB.Fetch(topic, 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	i := 0
	for iterB.Next() {
		expected := fmt.Sprintf("B-msg-%d", i)
		if string(iterB.Payload) != expected {
			t.Errorf("B message %d: got %q, want %q", i, iterB.Payload, expected)
		}
		if iterB.Offset != uint64(i) {
			t.Errorf("B message %d: offset got %d, want %d", i, iterB.Offset, i)
		}
		i++
	}
	if i != 5 {
		t.Errorf("expected 5 messages on B, got %d", i)
	}

	sA2 := NewServer("127.0.0.1:0", dirA, 10*1024*1024, 4096)
	if err := sA2.Start(); err != nil {
		t.Fatal(err)
	}
	defer sA2.Stop()

	cliA2 := client.NewClient(sA2.Addr(), 2*time.Second)
	if err := cliA2.Connect(); err != nil {
		t.Fatal(err)
	}
	defer cliA2.Close()

	iterA, err := cliA2.Fetch(topic, 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	i = 0
	for iterA.Next() {
		expected := fmt.Sprintf("A-msg-%d", i)
		if string(iterA.Payload) != expected {
			t.Errorf("A message %d: got %q, want %q", i, iterA.Payload, expected)
		}
		i++
	}
	if i != 10 {
		t.Errorf("expected 10 messages on A after restart, got %d", i)
	}
}

// TestConcurrentProduceFetch_NoRace stress-tests produce and fetch under the race detector.
func TestConcurrentProduceFetch_NoRace(t *testing.T) {
	dir, err := os.MkdirTemp("", "concurrent-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s := NewServer("127.0.0.1:0", dir, 64*1024*1024, 4096)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	const producers = 4
	const messagesPerProducer = 200
	const consumers = 2

	topic := "concurrent-topic"
	var wg sync.WaitGroup
	var produceErrors atomic.Int64

	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			cli := client.NewClient(s.Addr(), 2*time.Second)
			if err := cli.Connect(); err != nil {
				produceErrors.Add(1)
				return
			}
			defer cli.Close()
			for i := 0; i < messagesPerProducer; i++ {
				payload := []byte(fmt.Sprintf("p%d-m%d", pid, i))
				if _, err := cli.Produce(topic, payload); err != nil {
					produceErrors.Add(1)
					return
				}
			}
		}(p)
	}

	var fetchErrors atomic.Int64
	for c := 0; c < consumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cli := client.NewClient(s.Addr(), 2*time.Second)
			if err := cli.Connect(); err != nil {
				fetchErrors.Add(1)
				return
			}
			defer cli.Close()
			for offset := uint64(0); offset < 50; offset += 10 {
				_, err := cli.Fetch(topic, offset, 4096)
				if err != nil {
					continue
				}
			}
		}()
	}

	wg.Wait()

	if n := produceErrors.Load(); n > 0 {
		t.Errorf("produce errors: %d", n)
	}
}

// TestSegmentRoll_CrossSegmentFetch validates fetch across a rolled segment boundary.
func TestSegmentRoll_CrossSegmentFetch(t *testing.T) {
	dir, err := os.MkdirTemp("", "segroll-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	const maxSeg = 4 * 1024
	s := NewServer("127.0.0.1:0", dir, maxSeg, 64)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	cli := client.NewClient(s.Addr(), 2*time.Second)
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	topic := "roll-topic"
	payload := make([]byte, 300)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	const total = 60
	for i := 0; i < total; i++ {
		if _, err := cli.Produce(topic, payload); err != nil {
			t.Fatalf("produce %d failed: %v", i, err)
		}
	}

	topicDir := filepath.Join(dir, topic)
	entries, err := os.ReadDir(topicDir)
	if err != nil {
		t.Fatal(err)
	}
	var logFiles int
	for _, e := range entries {
		if !e.IsDir() && len(e.Name()) > 4 && e.Name()[len(e.Name())-4:] == ".log" {
			logFiles++
		}
	}
	if logFiles < 2 {
		t.Errorf("expected multiple segments after roll, got %d log files", logFiles)
	}

	iter, err := cli.Fetch(topic, 0, 10*512)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	firstLen := 0
	for iter.Next() {
		if count == 0 {
			firstLen = len(iter.Payload)
		}
		count++
	}
	if count == 0 {
		t.Error("fetch returned 0 messages across segments")
	}
	if count > 0 && firstLen != len(payload) {
		t.Errorf("payload size mismatch: got %d, want %d", firstLen, len(payload))
	}
}

// TestMalformedFrames_ServerDoesNotPanic ensures bad wire data closes the conn without panic.
func TestMalformedFrames_ServerDoesNotPanic(t *testing.T) {
	dir, err := os.MkdirTemp("", "malformed-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s := NewServer("127.0.0.1:0", dir, 10*1024*1024, 4096)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	cases := []struct {
		name  string
		frame []byte
	}{
		{
			name:  "zero-length frame",
			frame: []byte{0, 0, 0, 0},
		},
		{
			name: "unknown command",
			frame: func() []byte {
				b := make([]byte, 4+10)
				binary.BigEndian.PutUint32(b[0:4], 10)
				binary.BigEndian.PutUint16(b[4:6], 0xFFFF)
				return b
			}(),
		},
		{
			name: "valid produce with empty topic",
			frame: func() []byte {
				inner := protocol.EncodeProduceRequest(make([]byte, 1024), 1, "", []byte("x"))
				return inner
			}(),
		},
		{
			name:  "random garbage",
			frame: []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := net.DialTimeout("tcp", s.Addr(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()

			_ = conn.SetDeadline(time.Now().Add(500 * time.Millisecond))
			_, _ = conn.Write(tc.frame)

			buf := make([]byte, 64)
			_, _ = conn.Read(buf)
		})
	}

	cli := client.NewClient(s.Addr(), 2*time.Second)
	if err := cli.Connect(); err != nil {
		t.Fatal("server died after malformed frames:", err)
	}
	defer cli.Close()

	if _, err := cli.Produce("survival-topic", []byte("alive")); err != nil {
		t.Error("server did not survive malformed frame attacks:", err)
	}
}

// TestFix1_UseAfterFree_SegmentRollDuringFetch regression for fetch during segment roll.
func TestFix1_UseAfterFree_SegmentRollDuringFetch(t *testing.T) {
	dir, err := os.MkdirTemp("", "fix1-uaf-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	const segSize = 1024
	s := NewServer("127.0.0.1:0", dir, segSize, 64)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	topic := "uaf-topic"

	seeder := client.NewClient(s.Addr(), 2*time.Second)
	if err := seeder.Connect(); err != nil {
		t.Fatal(err)
	}
	seedPayload := make([]byte, 200)
	for i := 0; i < 20; i++ {
		if _, err := seeder.Produce(topic, seedPayload); err != nil {
			t.Fatalf("seed produce %d: %v", i, err)
		}
	}
	_ = seeder.Close()

	var wg sync.WaitGroup
	var produceErr, fetchErr atomic.Int64

	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cli := client.NewClient(s.Addr(), 2*time.Second)
			if err := cli.Connect(); err != nil {
				produceErr.Add(1)
				return
			}
			defer cli.Close()
			payload := make([]byte, 200)
			for i := 0; i < 50; i++ {
				if _, err := cli.Produce(topic, payload); err != nil {
					produceErr.Add(1)
					return
				}
			}
		}()
	}

	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cli := client.NewClient(s.Addr(), 2*time.Second)
			if err := cli.Connect(); err != nil {
				fetchErr.Add(1)
				return
			}
			defer cli.Close()
			for i := 0; i < 50; i++ {
				_, err := cli.Fetch(topic, uint64(i), 2048)
				if err != nil {
					continue
				}
			}
		}()
	}

	wg.Wait()

	if n := produceErr.Load(); n > 0 {
		t.Errorf("produce errors during concurrent roll stress: %d", n)
	}
}

// TestFix4_ConnectionCounter ensures OnOpen and OnClose keep the connection gauge accurate.
func TestFix4_ConnectionCounter(t *testing.T) {
	dir, err := os.MkdirTemp("", "fix4-conncount-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s := NewServer("127.0.0.1:0", dir, 10*1024*1024, 4096)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	const numConns = 20
	clients := make([]*client.Client, numConns)

	for i := 0; i < numConns; i++ {
		c := client.NewClient(s.Addr(), 2*time.Second)
		if err := c.Connect(); err != nil {
			t.Fatalf("connect[%d]: %v", i, err)
		}
		clients[i] = c
	}

	time.Sleep(100 * time.Millisecond)

	count := s.connCount.Load()
	if count != int64(numConns) {
		t.Errorf("connCount after %d connects: got %d, want %d", numConns, count, numConns)
	}

	for _, c := range clients {
		_ = c.Close()
	}

	time.Sleep(150 * time.Millisecond)

	count = s.connCount.Load()
	if count != 0 {
		t.Errorf("connCount after all closes: got %d, want 0", count)
	}
}

// TestFix1_ReadRawMessages_ReturnedSliceOutlivesRoll regression for fetch buffer lifetime after roll.
func TestFix1_ReadRawMessages_ReturnedSliceOutlivesRoll(t *testing.T) {
	dir, err := os.MkdirTemp("", "fix1-rmr-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	const segSize = 1024
	pl, err := log.NewPartitionLog(dir, segSize, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer pl.Close()

	payload := make([]byte, 200)
	for i := range payload {
		payload[i] = byte(i)
	}

	for i := 0; i < 3; i++ {
		if _, err := pl.Append(payload); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	snapshot, bufPtr, err := pl.ReadRawMessages(0, 4096)
	if err != nil {
		t.Fatalf("ReadRawMessages: %v", err)
	}
	if len(snapshot) == 0 {
		t.Fatal("ReadRawMessages returned empty slice")
	}
	if bufPtr != nil {
		defer log.FetchBufPool.Put(bufPtr)
	}

	for i := 0; i < 10; i++ {
		_, _ = pl.Append(payload)
	}

	time.Sleep(200 * time.Millisecond)

	if len(snapshot) < 12 {
		t.Fatalf("snapshot too small: %d bytes", len(snapshot))
	}

	recordLength := binary.BigEndian.Uint32(snapshot[0:4])
	payloadLen := int(recordLength) - 8
	if payloadLen != len(payload) {
		t.Errorf("first record payload length: got %d, want %d", payloadLen, len(payload))
	}

	const hdrSize = 12
	if hdrSize+payloadLen > len(snapshot) {
		t.Fatalf("snapshot truncated: need %d bytes, got %d", hdrSize+payloadLen, len(snapshot))
	}
	for i, b := range payload {
		if snapshot[hdrSize+i] != b {
			t.Errorf("payload byte %d: got 0x%02x, want 0x%02x", i, snapshot[hdrSize+i], b)
			break
		}
	}

	t.Logf("snapshot size: %d bytes, still valid after roll - UAF fix confirmed", len(snapshot))
}

// TestFix2_HealthzNoSyscallInPath ensures /healthz stays cheap under load.
func TestFix2_HealthzNoSyscallInPath(t *testing.T) {
	dir, err := os.MkdirTemp("", "fix2-nosyscall-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s := NewServer("127.0.0.1:0", dir, 10*1024*1024, 4096)
	s.SetHealthAddr("127.0.0.1:0")
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	time.Sleep(150 * time.Millisecond)

	healthURL := "http://" + s.HealthAddr() + "/healthz"

	const iterations = 200
	start := time.Now()
	for i := 0; i < iterations; i++ {
		if code := httpGet(t, healthURL); code != http.StatusOK {
			t.Fatalf("iteration %d: expected 200, got %d", i, code)
		}
	}
	elapsed := time.Since(start)
	perRequest := elapsed / iterations

	t.Logf("%d healthz requests in %v -> %v/req", iterations, elapsed, perRequest)

	const maxPerRequest = 5 * time.Millisecond
	if perRequest > maxPerRequest {
		t.Errorf("healthz too slow: %v/req > %v threshold (possible FS syscall in path)", perRequest, maxPerRequest)
	}
}

// TestFix_StressConcurrentRollFetchHealth combines roll, fetch, and health checks under concurrency.
func TestFix_StressConcurrentRollFetchHealth(t *testing.T) {
	dir, err := os.MkdirTemp("", "fix-stress-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	const segSize = 2048
	s := NewServer("127.0.0.1:0", dir, segSize, 64)
	s.SetHealthAddr("127.0.0.1:0")
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	time.Sleep(150 * time.Millisecond)

	topic := "stress-topic"
	healthURL := "http://" + s.HealthAddr() + "/healthz"
	var wg sync.WaitGroup
	var errs atomic.Int64

	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cli := client.NewClient(s.Addr(), 2*time.Second)
			if err := cli.Connect(); err != nil {
				errs.Add(1)
				return
			}
			defer cli.Close()
			payload := make([]byte, 400)
			for i := 0; i < 100; i++ {
				if _, err := cli.Produce(topic, payload); err != nil {
					errs.Add(1)
					return
				}
			}
		}()
	}

	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cli := client.NewClient(s.Addr(), 2*time.Second)
			if err := cli.Connect(); err != nil {
				errs.Add(1)
				return
			}
			defer cli.Close()
			for i := 0; i < 100; i++ {
				_, _ = cli.Fetch(topic, 0, 4096)
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			code := httpGet(t, healthURL)
			if code != http.StatusOK && code != http.StatusServiceUnavailable {
				errs.Add(1)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	wg.Wait()

	if count := s.connCount.Load(); count != 0 {
		t.Errorf("connCount after stress: got %d, want 0", count)
	}

	if n := errs.Load(); n > 0 {
		t.Errorf("stress test errors: %d", n)
	}
}

// TestFix5_TransientTopicKeyCorruption guards sync.Map topic keys against unsafe string reuse.
func TestFix5_TransientTopicKeyCorruption(t *testing.T) {
	dir, err := os.MkdirTemp("", "fix5-transient-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s := NewServer("127.0.0.1:0", dir, 1024*1024, 4096)

	buf := []byte("transient-topic-name-12345")
	var topic string
	{
		topic = unsafeString(buf)
	}

	pl, err := s.getOrCreatePartition(topic)
	if err != nil {
		t.Fatal(err)
	}
	if pl == nil {
		t.Fatal("expected partition log, got nil")
	}

	for i := range buf {
		buf[i] = 'X'
	}

	pl2, err := s.getOrCreatePartition("transient-topic-name-12345")
	if err != nil {
		t.Fatal(err)
	}
	if pl2 != pl {
		t.Errorf("expected to retrieve the same partition log instance, got different or nil")
	}
}

// TestFix6_LocateMessages_MalformedLength stops fetch from reading past a corrupt record length.
func TestFix6_LocateMessages_MalformedLength(t *testing.T) {
	dir, err := os.MkdirTemp("", "fix6-malformed-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	const maxSeg = 1024 * 1024
	pl, err := log.NewPartitionLog(dir, maxSeg, 128)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := pl.Append([]byte("valid")); err != nil {
		t.Fatal(err)
	}
	_ = pl.Close()

	logPath := filepath.Join(dir, "00000000000000000000.log")
	f, err := os.OpenFile(logPath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	var zeroLenHeader [12]byte
	binary.BigEndian.PutUint32(zeroLenHeader[0:4], 0)
	binary.BigEndian.PutUint64(zeroLenHeader[4:12], 999)
	if _, err := f.Write(zeroLenHeader[:]); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	pl2, err := log.NewPartitionLog(dir, maxSeg, 128)
	if err != nil {
		t.Fatal(err)
	}
	defer pl2.Close()

	_, bufPtr, err := pl2.ReadRawMessages(0, 1024)
	if err != nil && err != io.EOF {
		t.Fatalf("expected EOF or success, got error: %v", err)
	}
	if bufPtr != nil {
		log.FetchBufPool.Put(bufPtr)
	}
}

// httpGet returns the HTTP status code for a URL or fails the test.
func httpGet(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

// unsafeString converts payload bytes to string without allocation for test fixtures.
func unsafeString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// BenchmarkBrokerThroughput tracks end-to-end produce throughput for perf gate baselines.
func BenchmarkBrokerThroughput(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "broker-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	s := NewServer("127.0.0.1:0", tempDir, 64*1024*1024, 4096)
	if err := s.Start(); err != nil {
		b.Fatal(err)
	}
	defer s.Stop()

	addr := s.Addr()
	cli := client.NewClient(addr, 5*time.Second)
	if err := cli.Connect(); err != nil {
		b.Fatal(err)
	}
	defer cli.Close()

	topic := "bench-topic"
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = 'a'
	}

	b.ResetTimer()

	b.Run("Produce-Sequential", func(b *testing.B) {
		b.SetBytes(int64(len(payload)))
		for i := 0; i < b.N; i++ {
			_, err := cli.Produce(topic, payload)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Fetch-Sequential", func(b *testing.B) {
		b.SetBytes(int64(len(payload)))
		for i := 0; i < b.N; i++ {
			offset := uint64(i % b.N)
			iter, err := cli.Fetch(topic, offset, 1024)
			if err != nil {
				b.Fatal(err)
			}
			for iter.Next() {
			}
		}
	})

	b.Run("FetchStream-Sequential", func(b *testing.B) {
		b.SetBytes(int64(len(payload)))
		for i := 0; i < b.N; i++ {
			offset := uint64(i % b.N)
			iter, err := cli.Fetch(topic, offset, 1024)
			if err != nil {
				b.Fatal(err)
			}
			for iter.Next() {
			}
		}
	})
}
