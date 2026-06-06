package client

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mykhailov-ua/ad-event-processor/pkg/broker/protocol"
	"github.com/redis/go-redis/v9"
)

type Message struct {
	Offset  uint64
	Payload []byte
}

type Client struct {
	addr     string
	conn     net.Conn
	mu       sync.Mutex
	nextSeq  uint64
	readBuf  []byte
	writeBuf []byte
	lenBuf   []byte
	timeout  time.Duration
	redisURL string
	rdb      *redis.Client
}

func NewClient(addr string, timeout time.Duration) *Client {
	return &Client{
		addr:     addr,
		timeout:  timeout,
		readBuf:  make([]byte, 1024*1024),
		writeBuf: make([]byte, 1024*1024),
		lenBuf:   make([]byte, 4),
	}
}

func (c *Client) SetRedisURL(url string) {
	c.redisURL = url
}

func (c *Client) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connectLocked()
}

func (c *Client) connectLocked() error {
	if c.conn != nil {
		return nil
	}

	conn, err := net.DialTimeout("tcp", c.addr, c.timeout)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var err error
	if c.conn != nil {
		err = c.conn.Close()
		c.conn = nil
	}
	if c.rdb != nil {
		_ = c.rdb.Close()
		c.rdb = nil
	}
	return err
}

func (c *Client) getConn() (net.Conn, error) {
	if c.conn == nil {
		if err := c.connectLocked(); err != nil {
			return nil, err
		}
	}
	return c.conn, nil
}

func (c *Client) Produce(topic string, payload []byte) (uint64, error) {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			time.Sleep(500 * time.Millisecond)
		}

		c.mu.Lock()
		conn, err := c.getConn()
		if err != nil {
			c.mu.Unlock()
			lastErr = err

			if c.redisURL != "" {
				if newAddr, rErr := c.resolveLeaderAddr(topic); rErr == nil && newAddr != c.addr {
					c.addr = newAddr
				}
			}
			continue
		}

		seq := atomic.AddUint64(&c.nextSeq, 1)
		req := protocol.EncodeProduceRequest(c.writeBuf, seq, topic, payload)

		if c.timeout > 0 {
			_ = conn.SetDeadline(time.Now().Add(c.timeout))
		}

		if _, err := conn.Write(req); err != nil {
			_ = c.closeRawConn()
			c.mu.Unlock()
			lastErr = err
			if c.redisURL != "" {
				if newAddr, rErr := c.resolveLeaderAddr(topic); rErr == nil && newAddr != c.addr {
					c.addr = newAddr
				}
			}
			continue
		}

		cmd, respSeq, respPayload, err := protocol.ReadFrame(conn, c.readBuf, c.lenBuf)
		if err != nil {
			_ = c.closeRawConn()
			c.mu.Unlock()
			lastErr = err
			if c.redisURL != "" {
				if newAddr, rErr := c.resolveLeaderAddr(topic); rErr == nil && newAddr != c.addr {
					c.addr = newAddr
				}
			}
			continue
		}

		if cmd != protocol.CmdProduceResp {
			c.mu.Unlock()
			return 0, fmt.Errorf("unexpected command response: %d", cmd)
		}

		if respSeq != seq {
			c.mu.Unlock()
			return 0, fmt.Errorf("sequence mismatch: expected %d, got %d", seq, respSeq)
		}

		if len(respPayload) < 9 {
			c.mu.Unlock()
			return 0, errors.New("malformed produce response payload")
		}

		status := respPayload[0]
		if status == 4 {
			_ = c.closeRawConn()
			c.mu.Unlock()
			lastErr = errors.New("not leader")
			if c.redisURL != "" {
				if newAddr, rErr := c.resolveLeaderAddr(topic); rErr == nil && newAddr != c.addr {
					c.addr = newAddr
				}
			}
			continue
		}

		if status != 0 {
			c.mu.Unlock()
			return 0, fmt.Errorf("broker error status: %d", status)
		}

		offsetVal := binary.BigEndian.Uint64(respPayload[1:9])
		c.mu.Unlock()
		return offsetVal, nil
	}

	return 0, fmt.Errorf("failed after 5 attempts, last error: %w", lastErr)
}

func (c *Client) Fetch(topic string, startOffset uint64, maxBytes uint32) ([]Message, error) {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			time.Sleep(500 * time.Millisecond)
		}

		c.mu.Lock()
		conn, err := c.getConn()
		if err != nil {
			c.mu.Unlock()
			lastErr = err
			if c.redisURL != "" {
				if newAddr, rErr := c.resolveLeaderAddr(topic); rErr == nil && newAddr != c.addr {
					c.addr = newAddr
				}
			}
			continue
		}

		seq := atomic.AddUint64(&c.nextSeq, 1)
		req := protocol.EncodeFetchRequest(c.writeBuf, seq, topic, startOffset, maxBytes)

		if c.timeout > 0 {
			_ = conn.SetDeadline(time.Now().Add(c.timeout))
		}

		if _, err := conn.Write(req); err != nil {
			_ = c.closeRawConn()
			c.mu.Unlock()
			lastErr = err
			if c.redisURL != "" {
				if newAddr, rErr := c.resolveLeaderAddr(topic); rErr == nil && newAddr != c.addr {
					c.addr = newAddr
				}
			}
			continue
		}

		cmd, respSeq, respPayload, err := protocol.ReadFrame(conn, c.readBuf, c.lenBuf)
		if err != nil {
			_ = c.closeRawConn()
			c.mu.Unlock()
			lastErr = err
			if c.redisURL != "" {
				if newAddr, rErr := c.resolveLeaderAddr(topic); rErr == nil && newAddr != c.addr {
					c.addr = newAddr
				}
			}
			continue
		}

		if cmd != protocol.CmdFetchResp {
			c.mu.Unlock()
			return nil, fmt.Errorf("unexpected command response: %d", cmd)
		}

		if respSeq != seq {
			c.mu.Unlock()
			return nil, fmt.Errorf("sequence mismatch: expected %d, got %d", seq, respSeq)
		}

		if len(respPayload) < 5 {
			c.mu.Unlock()
			return nil, errors.New("malformed fetch response payload")
		}

		status := respPayload[0]
		if status == 4 {
			_ = c.closeRawConn()
			c.mu.Unlock()
			lastErr = errors.New("not leader")
			if c.redisURL != "" {
				if newAddr, rErr := c.resolveLeaderAddr(topic); rErr == nil && newAddr != c.addr {
					c.addr = newAddr
				}
			}
			continue
		}

		if status != 0 {
			c.mu.Unlock()
			return nil, fmt.Errorf("broker error status: %d", status)
		}

		msgCount := binary.BigEndian.Uint32(respPayload[1:5])
		messagesData := respPayload[5:]

		if msgCount == 0 {
			c.mu.Unlock()
			return nil, nil
		}

		messages := make([]Message, 0, msgCount)
		idx := 0
		for i := uint32(0); i < msgCount; i++ {
			if idx+12 > len(messagesData) {
				break
			}
			length := binary.BigEndian.Uint32(messagesData[idx : idx+4])
			offset := binary.BigEndian.Uint64(messagesData[idx+4 : idx+12])
			payloadLen := int(length) - 8

			if idx+12+payloadLen > len(messagesData) {
				break
			}

			payload := make([]byte, payloadLen)
			copy(payload, messagesData[idx+12:idx+12+payloadLen])
			messages = append(messages, Message{
				Offset:  offset,
				Payload: payload,
			})

			idx += 12 + payloadLen
		}

		c.mu.Unlock()
		return messages, nil
	}

	return nil, fmt.Errorf("failed after 5 attempts, last error: %w", lastErr)
}

func (c *Client) FetchStream(topic string, startOffset uint64, maxBytes uint32, cb func(offset uint64, payload []byte) error) error {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			time.Sleep(500 * time.Millisecond)
		}

		c.mu.Lock()
		conn, err := c.getConn()
		if err != nil {
			c.mu.Unlock()
			lastErr = err
			if c.redisURL != "" {
				if newAddr, rErr := c.resolveLeaderAddr(topic); rErr == nil && newAddr != c.addr {
					c.addr = newAddr
				}
			}
			continue
		}

		seq := atomic.AddUint64(&c.nextSeq, 1)
		req := protocol.EncodeFetchRequest(c.writeBuf, seq, topic, startOffset, maxBytes)

		if c.timeout > 0 {
			_ = conn.SetDeadline(time.Now().Add(c.timeout))
		}

		if _, err := conn.Write(req); err != nil {
			_ = c.closeRawConn()
			c.mu.Unlock()
			lastErr = err
			if c.redisURL != "" {
				if newAddr, rErr := c.resolveLeaderAddr(topic); rErr == nil && newAddr != c.addr {
					c.addr = newAddr
				}
			}
			continue
		}

		cmd, respSeq, respPayload, err := protocol.ReadFrame(conn, c.readBuf, c.lenBuf)
		if err != nil {
			_ = c.closeRawConn()
			c.mu.Unlock()
			lastErr = err
			if c.redisURL != "" {
				if newAddr, rErr := c.resolveLeaderAddr(topic); rErr == nil && newAddr != c.addr {
					c.addr = newAddr
				}
			}
			continue
		}

		if cmd != protocol.CmdFetchResp {
			c.mu.Unlock()
			return fmt.Errorf("unexpected command response: %d", cmd)
		}

		if respSeq != seq {
			c.mu.Unlock()
			return fmt.Errorf("sequence mismatch: expected %d, got %d", seq, respSeq)
		}

		if len(respPayload) < 5 {
			c.mu.Unlock()
			return errors.New("malformed fetch response payload")
		}

		status := respPayload[0]
		if status == 4 {
			_ = c.closeRawConn()
			c.mu.Unlock()
			lastErr = errors.New("not leader")
			if c.redisURL != "" {
				if newAddr, rErr := c.resolveLeaderAddr(topic); rErr == nil && newAddr != c.addr {
					c.addr = newAddr
				}
			}
			continue
		}

		if status != 0 {
			c.mu.Unlock()
			return fmt.Errorf("broker error status: %d", status)
		}

		msgCount := binary.BigEndian.Uint32(respPayload[1:5])
		messagesData := respPayload[5:]

		if msgCount == 0 {
			c.mu.Unlock()
			return nil
		}

		idx := 0
		for i := uint32(0); i < msgCount; i++ {
			if idx+12 > len(messagesData) {
				break
			}
			length := binary.BigEndian.Uint32(messagesData[idx : idx+4])
			offset := binary.BigEndian.Uint64(messagesData[idx+4 : idx+12])
			payloadLen := int(length) - 8

			if idx+12+payloadLen > len(messagesData) {
				break
			}

			payload := messagesData[idx+12 : idx+12+payloadLen]
			if err := cb(offset, payload); err != nil {
				c.mu.Unlock()
				return err
			}

			idx += 12 + payloadLen
		}

		c.mu.Unlock()
		return nil
	}

	return fmt.Errorf("failed after 5 attempts, last error: %w", lastErr)
}

func (c *Client) closeRawConn() error {
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}

func (c *Client) resolveLeaderAddr(topic string) (string, error) {
	if c.redisURL == "" {
		return "", errors.New("redis URL not set")
	}
	if c.rdb == nil {
		opts, err := redis.ParseURL(c.redisURL)
		if err != nil {
			return "", err
		}
		c.rdb = redis.NewClient(opts)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	leaderID, err := c.rdb.Get(ctx, "espx:topics:"+topic+":leader").Result()
	if err != nil {
		return "", err
	}
	return c.rdb.Get(ctx, "espx:brokers:"+leaderID).Result()
}
