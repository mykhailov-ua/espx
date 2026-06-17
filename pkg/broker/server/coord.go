package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"espx/pkg/broker/client"
	"github.com/redis/go-redis/v9"
)

// Coordinator elects per-topic leaders in Redis and tails the leader log on followers.
type Coordinator struct {
	nodeID    string
	tcpAddr   string
	rdb       redis.UniversalClient
	server    *Server
	closeChan chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
	leaders   atomic.Pointer[map[string]bool]
}

func NewCoordinator(nodeID string, tcpAddr string, redisURL string, server *Server) (*Coordinator, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse redis URL: %w", err)
	}

	rdb := redis.NewClient(opts)
	c := &Coordinator{
		nodeID:    nodeID,
		tcpAddr:   tcpAddr,
		rdb:       rdb,
		server:    server,
		closeChan: make(chan struct{}),
	}
	initMap := make(map[string]bool)
	c.leaders.Store(&initMap)
	return c, nil
}

func (c *Coordinator) Start() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.runHeartbeatLoop()
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.runCoordinationLoop()
	}()
}

func (c *Coordinator) Stop() {
	c.closeOnce.Do(func() {
		close(c.closeChan)
	})
	c.wg.Wait()
	_ = c.rdb.Close()
}

func (c *Coordinator) IsLeader(topic string) bool {
	m := c.leaders.Load()
	if m == nil {
		return false
	}
	return (*m)[topic]
}

func (c *Coordinator) HasLeader(topic string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	exists, err := c.rdb.Exists(ctx, "espx:topics:"+topic+":leader").Result()
	return exists > 0, err
}

func (c *Coordinator) runHeartbeatLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.closeChan:
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = c.rdb.Del(ctx, "espx:brokers:"+c.nodeID).Err()
			cancel()
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = c.rdb.Set(ctx, "espx:brokers:"+c.nodeID, c.tcpAddr, 5*time.Second).Err()
			cancel()
		}
	}
}

func (c *Coordinator) runCoordinationLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	replications := make(map[string]chan struct{})

	for {
		select {
		case <-c.closeChan:
			for _, stopCh := range replications {
				close(stopCh)
			}
			return
		case <-ticker.C:
			var topics []string
			c.server.topics.Range(func(key, _ any) bool {
				topics = append(topics, key.(string))
				return true
			})

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			for _, topic := range topics {
				leaderKey := "espx:topics:" + topic + ":leader"

				ok, err := c.rdb.SetNX(ctx, leaderKey, c.nodeID, 5*time.Second).Result()
				if err != nil {
					continue
				}

				if ok {
					c.setLeaderStatus(topic, true)
					if stopCh, exists := replications[topic]; exists {
						close(stopCh)
						delete(replications, topic)
					}
					_ = c.rdb.Expire(ctx, leaderKey, 5*time.Second).Err()
				} else {
					currentLeader, err := c.rdb.Get(ctx, leaderKey).Result()
					if err == nil && currentLeader == c.nodeID {
						c.setLeaderStatus(topic, true)
						_ = c.rdb.Expire(ctx, leaderKey, 5*time.Second).Err()
					} else {
						if _, exists := replications[topic]; !exists {
							stopCh := make(chan struct{})
							replications[topic] = stopCh
							c.wg.Add(1)
							go func(t string, sCh chan struct{}) {
								defer c.wg.Done()
								c.replicate(t, currentLeader, sCh)
							}(topic, stopCh)
						}
					}
				}
			}
			cancel()
		}
	}
}

func (c *Coordinator) setLeaderStatus(topic string, isLeader bool) {
	for {
		old := c.leaders.Load()
		newMap := make(map[string]bool, len(*old)+1)
		for k, v := range *old {
			newMap[k] = v
		}
		newMap[topic] = isLeader
		if c.leaders.CompareAndSwap(old, &newMap) {
			return
		}
	}
}

func (c *Coordinator) replicate(topic string, leaderID string, stopCh chan struct{}) {
	slog.Info("Starting replication", "topic", topic, "leader", leaderID)
	defer slog.Info("Stopped replication", "topic", topic)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var cli *client.Client
	var currentAddr string

	defer func() {
		if cli != nil {
			_ = cli.Close()
		}
	}()

	for {
		select {
		case <-stopCh:
			return
		case <-c.closeChan:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			leaderAddr, err := c.rdb.Get(ctx, "espx:brokers:"+leaderID).Result()
			cancel()
			if err != nil {
				if cli != nil {
					_ = cli.Close()
					cli = nil
				}
				continue
			}

			if cli == nil || leaderAddr != currentAddr {
				if cli != nil {
					_ = cli.Close()
				}
				cli = client.NewClient(leaderAddr, time.Second)
				currentAddr = leaderAddr
				if err := cli.Connect(); err != nil {
					_ = cli.Close()
					cli = nil
					continue
				}
			}

			pl, err := c.server.getOrCreatePartition(topic)
			if err != nil {
				continue
			}

			nextOffset := pl.NextOffset()

			iter, fetchErr := cli.Fetch(topic, nextOffset, 65536)
			if fetchErr == nil {
				for iter.Next() {
					if _, err = pl.Append(iter.Payload); err != nil {
						fetchErr = err
						break
					}
				}
			}

			if fetchErr != nil {
				_ = cli.Close()
				cli = nil
				if !errors.Is(fetchErr, errors.New("EOF")) {
					time.Sleep(500 * time.Millisecond)
				}
			}
		}
	}
}
