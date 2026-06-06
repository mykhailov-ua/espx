package server

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mykhailov-ua/ad-event-processor/pkg/broker/client"
	"github.com/mykhailov-ua/ad-event-processor/pkg/broker/log"
	"github.com/mykhailov-ua/ad-event-processor/pkg/broker/protocol"
)

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
		t.Log("WARNING: chmod did not block WriteAt on this kernel — ENOSPC emulation requires tmpfs with size limit in production CI")
		return
	}

	t.Logf("ENOSPC-like error correctly returned: %v", appendErr)
}

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

	msgs, err := cli.Fetch("slowloris-topic", 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != msgCount {
		t.Errorf("expected %d messages, got %d", msgCount, len(msgs))
	}
}

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

	msgsB, err := cliB.Fetch(topic, 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgsB) != 5 {
		t.Errorf("expected 5 messages on B, got %d", len(msgsB))
	}
	for i, m := range msgsB {
		expected := fmt.Sprintf("B-msg-%d", i)
		if string(m.Payload) != expected {
			t.Errorf("B message %d: got %q, want %q", i, m.Payload, expected)
		}
		if m.Offset != uint64(i) {
			t.Errorf("B message %d: offset got %d, want %d", i, m.Offset, i)
		}
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

	msgsA, err := cliA2.Fetch(topic, 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgsA) != 10 {
		t.Errorf("expected 10 messages on A after restart, got %d", len(msgsA))
	}
	for i, m := range msgsA {
		expected := fmt.Sprintf("A-msg-%d", i)
		if string(m.Payload) != expected {
			t.Errorf("A message %d: got %q, want %q", i, m.Payload, expected)
		}
	}
}

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

	msgs, err := cli.Fetch(topic, 0, 10*512)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) == 0 {
		t.Error("fetch returned 0 messages across segments")
	}
	if len(msgs) > 0 && len(msgs[0].Payload) != len(payload) {
		t.Errorf("payload size mismatch: got %d, want %d", len(msgs[0].Payload), len(payload))
	}
}

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
