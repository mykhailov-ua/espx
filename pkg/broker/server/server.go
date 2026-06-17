// Package server is the gnet-based broker TCP front-end with optional HA coordination.
package server

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"espx/pkg/broker/log"
	"espx/pkg/broker/protocol"
	"github.com/panjf2000/gnet/v2"
)

const MaxConnections int64 = 0

var bytePool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 32)
		return &b
	},
}

type Server struct {
	*gnet.BuiltinEventEngine
	addr          string
	healthAddr    string
	dataDir       string
	maxSegSize    int64
	indexInterval int64
	topics        sync.Map
	initMu        sync.Mutex
	closeChan     chan struct{}
	closeOnce     sync.Once
	wg            sync.WaitGroup
	engMu         sync.Mutex
	eng           gnet.Engine
	active        atomic.Bool
	httpSrv       *http.Server
	coord         *Coordinator
	registry      *protocol.TopicRegistry

	connCount atomic.Int64

	diskOK atomic.Bool
}

func NewServer(addr string, dataDir string, maxSegSize int64, indexInterval int64) *Server {
	s := &Server{
		BuiltinEventEngine: &gnet.BuiltinEventEngine{},
		addr:               addr,
		healthAddr:         "127.0.0.1:0",
		dataDir:            dataDir,
		maxSegSize:         maxSegSize,
		indexInterval:      indexInterval,
		closeChan:          make(chan struct{}),
		registry:           protocol.NewTopicRegistry(),
	}
	s.diskOK.Store(true)
	return s
}

func (s *Server) SetHealthAddr(addr string) {
	s.healthAddr = addr
}

func (s *Server) SetCoordinator(coord *Coordinator) {
	s.coord = coord
}

func (s *Server) HealthAddr() string {
	return s.healthAddr
}

func (s *Server) Start() error {
	if strings.HasSuffix(s.addr, ":0") {
		l, err := net.Listen("tcp", s.addr)
		if err != nil {
			return err
		}
		s.addr = l.Addr().String()
		_ = l.Close()
	}

	if strings.HasSuffix(s.healthAddr, ":0") {
		l, err := net.Listen("tcp", s.healthAddr)
		if err != nil {
			return err
		}
		s.healthAddr = l.Addr().String()
		_ = l.Close()
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runDiskHealthWorker()
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	s.httpSrv = &http.Server{
		Addr:    s.healthAddr,
		Handler: mux,
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		_ = s.httpSrv.ListenAndServe()
	}()

	s.wg.Add(1)
	errChan := make(chan error, 1)
	go func() {
		defer s.wg.Done()
		addr := "tcp://" + s.addr
		err := gnet.Run(s, addr,
			gnet.WithMulticore(true),
		)
		if err != nil {
			errChan <- err
		}
	}()

	select {
	case err := <-errChan:
		return err
	case <-time.After(100 * time.Millisecond):
		return nil
	}
}

func (s *Server) Addr() string {
	return s.addr
}

func (s *Server) runDiskHealthWorker() {
	const interval = 5 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.closeChan:
			return
		case <-ticker.C:
			s.diskOK.Store(s.probeDisk())
		}
	}
}

func (s *Server) probeDisk() bool {
	testFile := filepath.Join(s.dataDir, ".healthcheck")
	f, err := os.OpenFile(testFile, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		return false
	}
	_ = f.Close()
	_ = os.Remove(testFile)
	return true
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if !s.active.Load() {
		http.Error(w, "server not active", http.StatusServiceUnavailable)
		return
	}

	if !s.diskOK.Load() {
		http.Error(w, "disk not writable", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

func (s *Server) Stop() {
	s.closeOnce.Do(func() {
		close(s.closeChan)
		if s.httpSrv != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = s.httpSrv.Shutdown(ctx)
			cancel()
		}
		if s.active.Load() {
			s.engMu.Lock()
			eng := s.eng
			s.engMu.Unlock()
			_ = eng.Stop(context.Background())
		}

		s.topics.Range(func(_, val any) bool {
			_ = val.(*log.PartitionLog).Close()
			return true
		})
	})
	s.wg.Wait()
}

func (s *Server) OnBoot(eng gnet.Engine) gnet.Action {
	s.engMu.Lock()
	s.eng = eng
	s.engMu.Unlock()
	s.active.Store(true)
	s.diskOK.Store(s.probeDisk())
	return gnet.None
}

func (s *Server) OnOpen(c gnet.Conn) ([]byte, gnet.Action) {
	new := s.connCount.Add(1)
	if MaxConnections > 0 && new > MaxConnections {
		s.connCount.Add(-1)
		return nil, gnet.Close
	}
	return nil, gnet.None
}

func (s *Server) OnClose(c gnet.Conn, err error) gnet.Action {
	s.connCount.Add(-1)
	return gnet.None
}

func (s *Server) OnTraffic(c gnet.Conn) gnet.Action {
	for {
		lenBuf, err := c.Peek(4)
		if err != nil {
			return gnet.None
		}
		length := binary.BigEndian.Uint32(lenBuf)

		if length < 14 {
			if _, err := c.Discard(int(4 + length)); err != nil {
				return gnet.Close
			}
			return gnet.Close
		}

		payloadBuf, err := c.Peek(int(4 + length))
		if err != nil {
			return gnet.None
		}

		if _, err := c.Discard(int(4 + length)); err != nil {
			return gnet.Close
		}

		framePayload := payloadBuf[4 : 4+length]
		cmd := binary.BigEndian.Uint16(framePayload[0:2])
		seq := binary.BigEndian.Uint64(framePayload[2:10])
		reqPayload := framePayload[10 : length-4]

		expected := binary.BigEndian.Uint32(framePayload[length-4:])
		calculated := crc32.ChecksumIEEE(reqPayload)
		if calculated != expected {
			return gnet.Close
		}

		switch cmd {
		case protocol.CmdProduce:
			s.handleProduce(c, seq, reqPayload)
		case protocol.CmdFetch:
			s.handleFetch(c, seq, reqPayload)
		case protocol.CmdProduceBatch:
			s.handleProduceBatch(c, seq, reqPayload)
		case protocol.CmdRegisterTopic:
			s.handleRegisterTopic(c, seq, reqPayload)
		default:
			return gnet.Close
		}
	}
}

func (s *Server) handleRegisterTopic(c gnet.Conn, seq uint64, payload []byte) {
	bufPtr := bytePool.Get().(*[]byte)
	defer bytePool.Put(bufPtr)
	buf := (*bufPtr)[:32]

	topicName, err := protocol.DecodeRegisterTopicRequest(payload)
	if err != nil {
		resp := protocol.EncodeRegisterTopicResponse(buf, seq, 1, 0)
		_, _ = c.Write(resp)
		return
	}

	id, err := s.registry.Register(topicName)
	if err != nil {
		resp := protocol.EncodeRegisterTopicResponse(buf, seq, 2, 0)
		_, _ = c.Write(resp)
		return
	}

	resp := protocol.EncodeRegisterTopicResponse(buf, seq, 0, id)
	_, _ = c.Write(resp)
}

func (s *Server) handleProduceBatch(c gnet.Conn, seq uint64, payload []byte) {
	bufPtr := bytePool.Get().(*[]byte)
	defer bytePool.Put(bufPtr)
	buf := (*bufPtr)[:32]

	it := protocol.NewBatchIterator(payload)
	var lastOffset uint64
	var status byte = 0

	for it.Next() {
		meta, exists := s.registry.Lookup(it.TopicID)
		if !exists {
			status = 2
			break
		}

		if s.coord != nil && !s.coord.IsLeader(meta.Name) {
			hasLeader, _ := s.coord.HasLeader(meta.Name)
			if hasLeader {
				status = 4
				break
			}
		}

		pl, err := s.getOrCreatePartition(meta.Name)
		if err != nil {
			status = 2
			break
		}

		offset, err := pl.Append(it.Payload)
		if err != nil {
			status = 3
			break
		}
		lastOffset = offset
	}

	resp := protocol.EncodeProduceBatchResponse(buf, seq, status, lastOffset)
	_, _ = c.Write(resp)
}

func (s *Server) handleProduce(c gnet.Conn, seq uint64, payload []byte) {
	bufPtr := bytePool.Get().(*[]byte)
	defer bytePool.Put(bufPtr)
	buf := (*bufPtr)[:32]

	topic, msgPayload, err := protocol.DecodeProduceRequest(payload)
	if err != nil {
		resp := protocol.EncodeProduceResponse(buf, seq, 1, 0)
		_, _ = c.Write(resp)
		return
	}

	if s.coord != nil && !s.coord.IsLeader(topic) {
		hasLeader, _ := s.coord.HasLeader(topic)
		if hasLeader {
			resp := protocol.EncodeProduceResponse(buf, seq, 4, 0)
			_, _ = c.Write(resp)
			return
		}
	}

	pl, err := s.getOrCreatePartition(topic)
	if err != nil {
		resp := protocol.EncodeProduceResponse(buf, seq, 2, 0)
		_, _ = c.Write(resp)
		return
	}

	offset, err := pl.Append(msgPayload)
	if err != nil {
		resp := protocol.EncodeProduceResponse(buf, seq, 3, 0)
		_, _ = c.Write(resp)
		return
	}

	resp := protocol.EncodeProduceResponse(buf, seq, 0, offset)
	_, _ = c.Write(resp)
}

func (s *Server) handleFetch(c gnet.Conn, seq uint64, payload []byte) {
	bufPtr := bytePool.Get().(*[]byte)
	defer bytePool.Put(bufPtr)
	buf := (*bufPtr)[:32]

	topic, startOffset, maxBytes, err := protocol.DecodeFetchRequest(payload)
	if err != nil {
		header := protocol.EncodeFetchResponseHeader(buf, seq, 1, 0, 0)
		_, _ = c.Write(header)
		checksum := crc32.ChecksumIEEE(header[14:19])
		var checksumBuf [4]byte
		binary.BigEndian.PutUint32(checksumBuf[:], checksum)
		_, _ = c.Write(checksumBuf[:])
		return
	}

	pl, err := s.getOrCreatePartition(topic)
	if err != nil {
		header := protocol.EncodeFetchResponseHeader(buf, seq, 2, 0, 0)
		_, _ = c.Write(header)
		checksum := crc32.ChecksumIEEE(header[14:19])
		var checksumBuf [4]byte
		binary.BigEndian.PutUint32(checksumBuf[:], checksum)
		_, _ = c.Write(checksumBuf[:])
		return
	}

	data, dataBufPtr, err := pl.ReadRawMessages(startOffset, maxBytes)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, log.ErrSegmentNotFound) {
			header := protocol.EncodeFetchResponseHeader(buf, seq, 0, 0, 0)
			_, _ = c.Write(header)
			checksum := crc32.ChecksumIEEE(header[14:19])
			var checksumBuf [4]byte
			binary.BigEndian.PutUint32(checksumBuf[:], checksum)
			_, _ = c.Write(checksumBuf[:])
			return
		}
		header := protocol.EncodeFetchResponseHeader(buf, seq, 3, 0, 0)
		_, _ = c.Write(header)
		checksum := crc32.ChecksumIEEE(header[14:19])
		var checksumBuf [4]byte
		binary.BigEndian.PutUint32(checksumBuf[:], checksum)
		_, _ = c.Write(checksumBuf[:])
		return
	}
	if dataBufPtr != nil {
		defer log.FetchBufPool.Put(dataBufPtr)
	}

	msgCount, totalBytes := countMessages(data)

	header := protocol.EncodeFetchResponseHeader(buf, seq, 0, msgCount, totalBytes)
	_, _ = c.Write(header)
	if totalBytes > 0 {
		_, _ = c.Write(data)
	}

	var checksum uint32
	if totalBytes > 0 {
		checksum = crc32.Update(0, crc32.IEEETable, header[14:19])
		checksum = crc32.Update(checksum, crc32.IEEETable, data)
	} else {
		checksum = crc32.ChecksumIEEE(header[14:19])
	}
	var checksumBuf [4]byte
	binary.BigEndian.PutUint32(checksumBuf[:], checksum)
	_, _ = c.Write(checksumBuf[:])
}

func countMessages(buf []byte) (uint32, uint32) {
	var count, total uint32
	pos := 0
	for pos+12 <= len(buf) {
		length := binary.BigEndian.Uint32(buf[pos : pos+4])
		recordLen := int(12 + int(length) - 8)
		if pos+recordLen > len(buf) {
			break
		}
		count++
		total += uint32(recordLen)
		pos += recordLen
	}
	return count, total
}

func (s *Server) getOrCreatePartition(topic string) (*log.PartitionLog, error) {
	if val, ok := s.topics.Load(topic); ok {
		return val.(*log.PartitionLog), nil
	}

	s.initMu.Lock()
	defer s.initMu.Unlock()

	if val, ok := s.topics.Load(topic); ok {
		return val.(*log.PartitionLog), nil
	}

	dir := filepath.Join(s.dataDir, topic)
	pl, err := log.NewPartitionLog(dir, s.maxSegSize, s.indexInterval)
	if err != nil {
		return nil, err
	}
	s.topics.Store(strings.Clone(topic), pl)
	return pl, nil
}
