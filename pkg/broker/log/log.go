// Package log implements mmap-backed append-only segments for broker partition storage.
package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

var ErrSegmentNotFound = errors.New("segment not found")

// FetchBufPool reuses 1 MiB fetch buffers to keep broker read paths allocation-free.
var FetchBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 1024*1024)
		return &b
	},
}

type Segment struct {
	baseOffset uint64
	logFile    *os.File
	indexFile  *os.File
	logPath    string
	indexPath  string
	logSize    int64
	indexSize  int64

	mmapData   []byte
	mmapIndex  []byte
	maxSegSize int64
	maxIdxSize int64
}

func findActualIndexSize(idxData []byte, baseOffset uint64) int64 {
	numEntries := len(idxData) / 16
	var count int64 = 0
	var lastOffset uint64 = 0
	hasLast := false

	for i := 0; i < numEntries; i++ {
		off := binary.BigEndian.Uint64(idxData[i*16 : i*16+8])
		pos := int64(binary.BigEndian.Uint64(idxData[i*16+8 : i*16+16]))

		if off == 0 && pos == 0 && i > 0 {
			break
		}
		if off < baseOffset {
			break
		}
		if pos < 0 {
			break
		}
		if hasLast && off <= lastOffset {
			break
		}

		lastOffset = off
		hasLast = true
		count++
	}

	return count * 16
}

func NewSegment(dir string, baseOffset uint64, maxSegSize int64, indexInterval int64, writeable bool) (*Segment, error) {
	logName := fmt.Sprintf("%020d.log", baseOffset)
	idxName := fmt.Sprintf("%020d.index", baseOffset)
	logPath := filepath.Join(dir, logName)
	indexPath := filepath.Join(dir, idxName)

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file %s: %w", logPath, err)
	}

	indexFile, err := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("failed to open index file %s: %w", indexPath, err)
	}

	logInfo, err := logFile.Stat()
	if err != nil {
		_ = logFile.Close()
		_ = indexFile.Close()
		return nil, err
	}

	logSize := logInfo.Size()
	idxInfo, err := indexFile.Stat()
	if err != nil {
		_ = logFile.Close()
		_ = indexFile.Close()
		return nil, err
	}
	indexSize := idxInfo.Size()

	if indexSize > 0 {
		idxData := make([]byte, indexSize)
		if _, err := io.ReadFull(indexFile, idxData); err == nil {
			indexSize = findActualIndexSize(idxData, baseOffset)
		}
		_, _ = indexFile.Seek(0, io.SeekStart)
	}

	if indexInterval <= 0 {
		indexInterval = 4096
	}
	maxIdxSize := (maxSegSize/indexInterval + 100) * 16

	var mmapData []byte
	var mmapIndex []byte

	if writeable {
		if logSize < maxSegSize {
			if err := logFile.Truncate(maxSegSize); err != nil {
				_ = logFile.Close()
				_ = indexFile.Close()
				return nil, err
			}
		}
		mmapData, err = syscall.Mmap(int(logFile.Fd()), 0, int(maxSegSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			_ = logFile.Close()
			_ = indexFile.Close()
			return nil, fmt.Errorf("log mmap failed: %w", err)
		}

		for i := 0; i < len(mmapData); i += 4096 {
			_ = mmapData[i]
		}

		if indexSize < maxIdxSize {
			if err := indexFile.Truncate(maxIdxSize); err != nil {
				_ = syscall.Munmap(mmapData)
				_ = logFile.Close()
				_ = indexFile.Close()
				return nil, err
			}
		}
		mmapIndex, err = syscall.Mmap(int(indexFile.Fd()), 0, int(maxIdxSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			_ = syscall.Munmap(mmapData)
			_ = logFile.Close()
			_ = indexFile.Close()
			return nil, fmt.Errorf("index mmap failed: %w", err)
		}

		for i := 0; i < len(mmapIndex); i += 4096 {
			_ = mmapIndex[i]
		}
	} else {
		if logSize > 0 {
			mmapData, err = syscall.Mmap(int(logFile.Fd()), 0, int(logSize), syscall.PROT_READ, syscall.MAP_SHARED)
			if err != nil {
				_ = logFile.Close()
				_ = indexFile.Close()
				return nil, fmt.Errorf("log mmap failed: %w", err)
			}
			madvise(mmapData, syscall.MADV_WILLNEED)
		}
		if indexSize > 0 {
			mmapIndex, err = syscall.Mmap(int(indexFile.Fd()), 0, int(indexSize), syscall.PROT_READ, syscall.MAP_SHARED)
			if err != nil {
				if len(mmapData) > 0 {
					_ = syscall.Munmap(mmapData)
				}
				_ = logFile.Close()
				_ = indexFile.Close()
				return nil, fmt.Errorf("index mmap failed: %w", err)
			}
			madvise(mmapIndex, syscall.MADV_WILLNEED)
		}
	}

	return &Segment{
		baseOffset: baseOffset,
		logFile:    logFile,
		indexFile:  indexFile,
		logPath:    logPath,
		indexPath:  indexPath,
		logSize:    logSize,
		indexSize:  indexSize,
		mmapData:   mmapData,
		mmapIndex:  mmapIndex,
		maxSegSize: maxSegSize,
		maxIdxSize: maxIdxSize,
	}, nil
}

func madvise(data []byte, advice int) {
	if len(data) == 0 {
		return
	}
	ptr := unsafe.Pointer(unsafe.SliceData(data))
	_, _, _ = syscall.Syscall(syscall.SYS_MADVISE, uintptr(ptr), uintptr(len(data)), uintptr(advice))
}

func (s *Segment) Close() error {
	var errs []error
	if len(s.mmapData) > 0 {
		if err := syscall.Munmap(s.mmapData); err != nil {
			errs = append(errs, err)
		}
		s.mmapData = nil
	}
	if len(s.mmapIndex) > 0 {
		if err := syscall.Munmap(s.mmapIndex); err != nil {
			errs = append(errs, err)
		}
		s.mmapIndex = nil
	}
	if err := s.logFile.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := s.indexFile.Close(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *Segment) Write(offset uint64, payload []byte) (int64, error) {
	payloadLen := len(payload)
	length := uint32(8 + payloadLen)
	totalLen := 12 + payloadLen
	pos := atomic.LoadInt64(&s.logSize)

	if pos+int64(totalLen) > s.maxSegSize {
		return 0, errors.New("segment space exhausted")
	}

	basePtr := unsafe.Pointer(unsafe.SliceData(s.mmapData))
	recordPtr := unsafe.Pointer(uintptr(basePtr) + uintptr(pos))

	*(*uint32)(recordPtr) = bits.ReverseBytes32(length)

	offsetPtr := unsafe.Pointer(uintptr(recordPtr) + 4)
	*(*uint64)(offsetPtr) = bits.ReverseBytes64(offset)

	if payloadLen > 0 {
		payloadDst := unsafe.Slice((*byte)(unsafe.Pointer(uintptr(recordPtr)+12)), payloadLen)
		copy(payloadDst, payload)
	}

	atomic.StoreInt64(&s.logSize, pos+int64(totalLen))
	return pos, nil
}

func (s *Segment) WriteIndexEntry(offset uint64, position int64) error {
	idxSize := atomic.LoadInt64(&s.indexSize)
	if idxSize+16 > s.maxIdxSize {
		return errors.New("index space exhausted")
	}

	basePtr := unsafe.Pointer(unsafe.SliceData(s.mmapIndex))
	entryPtr := unsafe.Pointer(uintptr(basePtr) + uintptr(idxSize))

	*(*uint64)(entryPtr) = bits.ReverseBytes64(offset)

	posPtr := unsafe.Pointer(uintptr(entryPtr) + 8)
	*(*uint64)(posPtr) = bits.ReverseBytes64(uint64(position))

	atomic.StoreInt64(&s.indexSize, idxSize+16)
	return nil
}

func (s *Segment) FindPosition(offset uint64) (int64, error) {
	idxSize := atomic.LoadInt64(&s.indexSize)
	n := idxSize / 16
	if n == 0 {
		return 0, nil
	}

	low := int64(0)
	high := n - 1
	var bestPos int64 = 0

	for low <= high {
		mid := (low + high) / 2
		off := binary.BigEndian.Uint64(s.mmapIndex[mid*16 : mid*16+8])
		if off <= offset {
			bestPos = int64(binary.BigEndian.Uint64(s.mmapIndex[mid*16+8 : mid*16+16]))
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return bestPos, nil
}

func (s *Segment) Recover() (uint64, error) {
	idxInfo, err := s.indexFile.Stat()
	if err != nil {
		return s.baseOffset, err
	}

	idxSize := idxInfo.Size()
	if idxSize > 0 {
		idxData := make([]byte, idxSize)
		if _, err := s.indexFile.ReadAt(idxData, 0); err == nil {
			idxSize = findActualIndexSize(idxData, s.baseOffset)
		}
	}

	var lastIdxOffset uint64 = s.baseOffset
	var lastIdxPos int64 = 0

	if idxSize >= 16 {
		if len(s.mmapIndex) >= int(idxSize) {
			lastIdxOffset = binary.BigEndian.Uint64(s.mmapIndex[idxSize-16 : idxSize-8])
			lastIdxPos = int64(binary.BigEndian.Uint64(s.mmapIndex[idxSize-8 : idxSize]))
		}
	}

	atomic.StoreInt64(&s.indexSize, idxSize)

	currentOffset := lastIdxOffset
	currentPos := lastIdxPos
	mmapSize := int64(len(s.mmapData))

	for {
		if currentPos+12 > mmapSize {
			break
		}

		length := binary.BigEndian.Uint32(s.mmapData[currentPos : currentPos+4])
		offset := binary.BigEndian.Uint64(s.mmapData[currentPos+4 : currentPos+12])

		if length == 0 && offset == 0 {
			break
		}

		payloadLen := int64(length) - 8
		if payloadLen < 0 || currentPos+12+payloadLen > mmapSize {
			break
		}

		currentOffset = offset + 1
		currentPos += 12 + payloadLen
	}

	atomic.StoreInt64(&s.logSize, currentPos)
	if err := s.logFile.Truncate(currentPos); err != nil {
		return currentOffset, err
	}

	if len(s.mmapData) > 0 {
		_ = syscall.Munmap(s.mmapData)
		s.mmapData = nil
	}
	if len(s.mmapIndex) > 0 {
		_ = syscall.Munmap(s.mmapIndex)
		s.mmapIndex = nil
	}

	if mmapSize == s.maxSegSize {
		if err := s.logFile.Truncate(s.maxSegSize); err != nil {
			return currentOffset, err
		}
		s.mmapData, err = syscall.Mmap(int(s.logFile.Fd()), 0, int(s.maxSegSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			return currentOffset, err
		}
		if err := s.indexFile.Truncate(s.maxIdxSize); err != nil {
			return currentOffset, err
		}
		s.mmapIndex, err = syscall.Mmap(int(s.indexFile.Fd()), 0, int(s.maxIdxSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	} else {
		if currentPos > 0 {
			s.mmapData, err = syscall.Mmap(int(s.logFile.Fd()), 0, int(currentPos), syscall.PROT_READ, syscall.MAP_SHARED)
			if err != nil {
				return currentOffset, err
			}
		}
		if idxSize > 0 {
			s.mmapIndex, err = syscall.Mmap(int(s.indexFile.Fd()), 0, int(idxSize), syscall.PROT_READ, syscall.MAP_SHARED)
		}
	}
	return currentOffset, err
}

func (s *Segment) LocateMessages(indexPos int64, startOffset uint64, maxBytes uint32) (int64, uint32, uint32, error) {
	logSize := atomic.LoadInt64(&s.logSize)
	currentPos := indexPos
	var targetPos int64 = -1
	var msgCount uint32 = 0
	var totalMsgBytes uint32 = 0

	for {
		if currentPos+12 > logSize {
			break
		}

		length := binary.BigEndian.Uint32(s.mmapData[currentPos : currentPos+4])
		offset := binary.BigEndian.Uint64(s.mmapData[currentPos+4 : currentPos+12])
		payloadLen := int64(length) - 8
		if payloadLen < 0 {
			break
		}
		recordLen := 12 + payloadLen

		if currentPos+recordLen > logSize {
			break
		}

		if offset >= startOffset {
			if targetPos == -1 {
				targetPos = currentPos
			}
			if targetPos != -1 {
				if totalMsgBytes+uint32(recordLen) > maxBytes && msgCount > 0 {
					return targetPos, msgCount, totalMsgBytes, nil
				}
				msgCount++
				totalMsgBytes += uint32(recordLen)
			}
		}
		currentPos += recordLen

		if targetPos != -1 && totalMsgBytes >= maxBytes {
			break
		}
	}

	if targetPos == -1 {
		return 0, 0, 0, io.EOF
	}

	return targetPos, msgCount, totalMsgBytes, nil
}

func (s *Segment) Sync() error {
	var errs []error
	if err := s.logFile.Sync(); err != nil {
		errs = append(errs, err)
	}
	if err := s.indexFile.Sync(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

type segmentSnapshot struct {
	segments  []*Segment
	activeSeg *Segment
}

type PartitionLog struct {
	writeMu       sync.Mutex
	dir           string
	snap          atomic.Pointer[segmentSnapshot]
	nextOffset    uint64
	bytesSinceIdx int64
	indexInterval int64
	maxSegSize    int64
	flushTicker   *time.Ticker
	closeChan     chan struct{}
	wg            sync.WaitGroup

	DiskOK atomic.Bool
}

func NewPartitionLog(dir string, maxSegSize int64, indexInterval int64) (*PartitionLog, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	p := &PartitionLog{
		dir:           dir,
		maxSegSize:    maxSegSize,
		indexInterval: indexInterval,
		closeChan:     make(chan struct{}),
	}
	p.DiskOK.Store(true)

	if err := p.loadSegments(); err != nil {
		return nil, err
	}

	p.startFlushLoop()
	return p, nil
}

func (p *PartitionLog) loadSegments() error {
	files, err := os.ReadDir(p.dir)
	if err != nil {
		return err
	}

	var baseOffsets []uint64
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		if strings.HasSuffix(f.Name(), ".log") {
			baseStr := strings.TrimSuffix(f.Name(), ".log")
			val, err := strconv.ParseUint(baseStr, 10, 64)
			if err != nil {
				continue
			}
			baseOffsets = append(baseOffsets, val)
		}
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	var segments []*Segment
	for i, offset := range baseOffsets {
		isLast := i == len(baseOffsets)-1
		seg, err := NewSegment(p.dir, offset, p.maxSegSize, p.indexInterval, isLast)
		if err != nil {
			return err
		}
		segments = append(segments, seg)
	}

	if len(segments) == 0 {
		seg, err := NewSegment(p.dir, 0, p.maxSegSize, p.indexInterval, true)
		if err != nil {
			return err
		}
		segments = append(segments, seg)
	}

	active := segments[len(segments)-1]

	next, err := active.Recover()
	if err != nil {
		return fmt.Errorf("failed to recover active segment: %w", err)
	}
	p.nextOffset = next

	p.snap.Store(&segmentSnapshot{
		segments:  segments,
		activeSeg: active,
	})

	return nil
}

func (p *PartitionLog) startFlushLoop() {
	p.flushTicker = time.NewTicker(100 * time.Millisecond)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			select {
			case <-p.flushTicker.C:
				p.Sync()
			case <-p.closeChan:
				return
			}
		}
	}()
}

func (p *PartitionLog) NextOffset() uint64 {
	p.writeMu.Lock()
	off := p.nextOffset
	p.writeMu.Unlock()
	return off
}

func (p *PartitionLog) Append(payload []byte) (uint64, error) {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	s := p.snap.Load()
	activeSeg := s.activeSeg
	offset := p.nextOffset
	totalLen := int64(12 + len(payload))

	activeLogSize := atomic.LoadInt64(&activeSeg.logSize)
	if activeLogSize+totalLen > p.maxSegSize {
		if err := p.rollLocked(s); err != nil {
			return 0, err
		}
		s = p.snap.Load()
		activeSeg = s.activeSeg
	}
	pos, err := activeSeg.Write(offset, payload)
	if err != nil {
		return 0, err
	}

	p.nextOffset++
	p.bytesSinceIdx += int64(12 + len(payload))

	if p.bytesSinceIdx >= p.indexInterval {
		if err := activeSeg.WriteIndexEntry(offset, pos); err != nil {
			return 0, err
		}
		p.bytesSinceIdx = 0
	}

	return offset, nil
}

func (p *PartitionLog) rollLocked(old *segmentSnapshot) error {
	activeSeg := old.activeSeg

	if err := activeSeg.Sync(); err != nil {
		return err
	}

	activeLogSize := atomic.LoadInt64(&activeSeg.logSize)
	if err := activeSeg.logFile.Truncate(activeLogSize); err != nil {
		return err
	}

	activeIdxSize := atomic.LoadInt64(&activeSeg.indexSize)
	if err := activeSeg.indexFile.Truncate(activeIdxSize); err != nil {
		return err
	}

	readOnlySeg, err := NewSegment(p.dir, activeSeg.baseOffset, p.maxSegSize, p.indexInterval, false)
	if err != nil {
		return err
	}

	newSeg, err := NewSegment(p.dir, p.nextOffset, p.maxSegSize, p.indexInterval, true)
	if err != nil {
		_ = readOnlySeg.Close()
		return err
	}

	newSegments := make([]*Segment, len(old.segments))
	copy(newSegments, old.segments)
	newSegments[len(old.segments)-1] = readOnlySeg
	newSegments = append(newSegments, newSeg)

	p.snap.Store(&segmentSnapshot{
		segments:  newSegments,
		activeSeg: newSeg,
	})

	go func(seg *Segment) {
		time.Sleep(100 * time.Millisecond)
		_ = seg.Close()
	}(activeSeg)

	p.bytesSinceIdx = 0
	return nil
}

func (p *PartitionLog) Sync() {
	s := p.snap.Load()
	if s != nil && s.activeSeg != nil {
		_ = s.activeSeg.Sync()
	}
}

func (p *PartitionLog) Close() error {
	close(p.closeChan)
	p.flushTicker.Stop()
	p.wg.Wait()

	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	s := p.snap.Load()
	if s == nil {
		return nil
	}

	var errs []error
	for _, seg := range s.segments {
		if err := seg.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// ReadRawMessages snapshots segments without RWMutex so mmap page faults cannot stall appends.
func (p *PartitionLog) ReadRawMessages(startOffset uint64, maxBytes uint32) ([]byte, *[]byte, error) {
	s := p.snap.Load()
	if s == nil || len(s.segments) == 0 {
		return nil, nil, ErrSegmentNotFound
	}

	var targetSeg *Segment
	for i := len(s.segments) - 1; i >= 0; i-- {
		if s.segments[i].baseOffset <= startOffset {
			targetSeg = s.segments[i]
			break
		}
	}

	if targetSeg == nil {
		return nil, nil, ErrSegmentNotFound
	}

	pos, err := targetSeg.FindPosition(startOffset)
	if err != nil {
		return nil, nil, err
	}

	targetPos, _, totalMsgBytes, err := targetSeg.LocateMessages(pos, startOffset, maxBytes)
	if err != nil {
		return nil, nil, err
	}

	if totalMsgBytes == 0 {
		return nil, nil, io.EOF
	}

	var bufPtr *[]byte
	var buf []byte
	if totalMsgBytes <= 1024*1024 {
		bufPtr = FetchBufPool.Get().(*[]byte)
		buf = (*bufPtr)[:totalMsgBytes]
	} else {
		buf = make([]byte, totalMsgBytes)
	}

	copy(buf, targetSeg.mmapData[targetPos:targetPos+int64(totalMsgBytes)])
	return buf, bufPtr, nil
}
