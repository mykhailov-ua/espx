package server

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"espx/pkg/broker/client"

	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
)

// TestHAClusterFailoverAndReplication validates leader election, replication, and failover produce.
func TestHAClusterFailoverAndReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	redisContainer, err := rediscontainer.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("failed to start redis container: %v", err)
	}
	defer func() {
		_ = redisContainer.Terminate(ctx)
	}()

	redisEndpoint, err := redisContainer.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("failed to get redis endpoint: %v", err)
	}
	redisURL := fmt.Sprintf("redis://%s/0", redisEndpoint)

	dir1, err := os.MkdirTemp("", "broker-ha-1-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir1)

	dir2, err := os.MkdirTemp("", "broker-ha-2-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir2)

	s1 := NewServer("127.0.0.1:0", dir1, 10*1024*1024, 4096)
	if err := s1.Start(); err != nil {
		t.Fatal(err)
	}
	defer s1.Stop()

	coord1, err := NewCoordinator("broker-1", s1.Addr(), redisURL, s1)
	if err != nil {
		t.Fatal(err)
	}
	s1.SetCoordinator(coord1)
	coord1.Start()
	defer coord1.Stop()

	s2 := NewServer("127.0.0.1:0", dir2, 10*1024*1024, 4096)
	if err := s2.Start(); err != nil {
		t.Fatal(err)
	}
	defer s2.Stop()

	coord2, err := NewCoordinator("broker-2", s2.Addr(), redisURL, s2)
	if err != nil {
		t.Fatal(err)
	}
	s2.SetCoordinator(coord2)
	coord2.Start()
	defer coord2.Stop()

	time.Sleep(3 * time.Second)

	topic := "ha-events"

	p1, err := s1.getOrCreatePartition(topic)
	if err != nil {
		t.Fatal(err)
	}
	_ = p1
	p2, err := s2.getOrCreatePartition(topic)
	if err != nil {
		t.Fatal(err)
	}
	_ = p2

	time.Sleep(2 * time.Second)

	l1 := coord1.IsLeader(topic)
	l2 := coord2.IsLeader(topic)

	if !l1 && !l2 {
		t.Fatal("expected one broker to be elected as leader")
	}

	var leaderServer *Server
	var followerServer *Server
	var leaderCoord *Coordinator

	if l1 {
		leaderServer = s1
		followerServer = s2
		leaderCoord = coord1
	} else {
		leaderServer = s2
		followerServer = s1
		leaderCoord = coord2
	}

	cli := client.NewClient(leaderServer.Addr(), 2*time.Second)
	cli.SetRedisURL(redisURL)
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	msgCount := 20
	for i := 0; i < msgCount; i++ {
		payload := []byte(fmt.Sprintf("ha-msg-payload-%d", i))
		offset, err := cli.Produce(topic, payload)
		if err != nil {
			t.Fatalf("produce failed on message %d: %v", i, err)
		}
		if offset != uint64(i) {
			t.Errorf("unexpected offset: got %d, expected %d", offset, i)
		}
	}

	time.Sleep(1 * time.Second)

	fPartition, err := followerServer.getOrCreatePartition(topic)
	if err != nil {
		t.Fatal(err)
	}
	if fPartition.NextOffset() != uint64(msgCount) {
		t.Errorf("expected follower to replicate %d messages, got %d", msgCount, fPartition.NextOffset())
	}

	leaderCoord.Stop()
	leaderServer.Stop()

	time.Sleep(6 * time.Second)

	payload := []byte("msg-after-failover")
	offset, err := cli.Produce(topic, payload)
	if err != nil {
		t.Fatalf("failover produce failed: %v", err)
	}
	expectedOffset := uint64(msgCount)
	if offset != expectedOffset {
		t.Errorf("unexpected offset after failover: got %d, expected %d", offset, expectedOffset)
	}

	newLeaderPartition, err := followerServer.getOrCreatePartition(topic)
	if err != nil {
		t.Fatal(err)
	}
	if newLeaderPartition.NextOffset() != expectedOffset+1 {
		t.Errorf("new leader next offset mismatch: got %d, expected %d", newLeaderPartition.NextOffset(), expectedOffset+1)
	}
}
