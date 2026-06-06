package server

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mykhailov-ua/ad-event-processor/pkg/broker/client"
)

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

	messages, err := cli.Fetch(topic, 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	if len(messages) != msgCount {
		t.Fatalf("expected to fetch %d messages, got %d", msgCount, len(messages))
	}

	for i, msg := range messages {
		expectedPayload := fmt.Sprintf("msg-payload-%d", i)
		if string(msg.Payload) != expectedPayload {
			t.Errorf("mismatch on message %d: got %q, expected %q", i, string(msg.Payload), expectedPayload)
		}
		if msg.Offset != uint64(i) {
			t.Errorf("offset mismatch: got %d, expected %d", msg.Offset, i)
		}
	}

	midMessages, err := cli.Fetch(topic, 25, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	if len(midMessages) != 25 {
		t.Fatalf("expected to fetch 25 messages, got %d", len(midMessages))
	}

	for i, msg := range midMessages {
		expectedIdx := i + 25
		expectedPayload := fmt.Sprintf("msg-payload-%d", expectedIdx)
		if string(msg.Payload) != expectedPayload {
			t.Errorf("mismatch on sub-fetch message %d: got %q, expected %q", expectedIdx, string(msg.Payload), expectedPayload)
		}
	}
}

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

		messages, err := cli.Fetch(topic, 0, 1024*1024)
		if err != nil {
			t.Fatal(err)
		}

		if len(messages) != 10 {
			t.Fatalf("expected 10 recovered messages, got %d", len(messages))
		}

		for i, msg := range messages {
			expectedPayload := fmt.Sprintf("rec-msg-%d", i)
			if string(msg.Payload) != expectedPayload {
				t.Errorf("recovered message %d mismatch: got %q, expected %q", i, string(msg.Payload), expectedPayload)
			}
		}
	}
}

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
			_, err := cli.Fetch(topic, offset, 1024)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("FetchStream-Sequential", func(b *testing.B) {
		b.SetBytes(int64(len(payload)))
		for i := 0; i < b.N; i++ {
			offset := uint64(i % b.N)
			err := cli.FetchStream(topic, offset, 1024, func(off uint64, msgPayload []byte) error {
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
