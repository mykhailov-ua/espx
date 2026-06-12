package logger

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/crypto/pbkdf2"
)

var (
	zstdEncoderPool = sync.Pool{
		New: func() any {
			enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
			if err != nil {
				panic(err)
			}
			return enc
		},
	}
	zstdDecoder *zstd.Decoder
)

func init() {
	var err error
	zstdDecoder, err = zstd.NewReader(nil)
	if err != nil {
		panic(err)
	}
}

type LogPayload struct {
	ready    atomic.Uint32
	Priority uint8
	Length   uint32
	Data     [500]byte
}

type LogShard struct {
	_           [64]byte
	writeCursor uint64 // published tail (visible to drainer)
	_           [64]byte
	allocCursor uint64 // reserved tail (producers CAS)
	_           [64]byte
	readCursor  uint64
	_           [64]byte
	slots       [65536]LogPayload
}

const (
	RingCapacity = 65536
	RingMask     = RingCapacity - 1
	ringUsable   = RingCapacity - 1
)

func NewLogShard() *LogShard {
	return &LogShard{}
}

func (s *LogShard) Write(priority uint8, data []byte) bool {
	for {
		alloc := atomic.LoadUint64(&s.allocCursor)
		read := atomic.LoadUint64(&s.readCursor)
		if alloc-read >= ringUsable {
			for spin := 0; spin < 100; spin++ {
				if spin < 20 {
					runtime.Gosched()
				} else {
					time.Sleep(time.Microsecond)
				}
				read = atomic.LoadUint64(&s.readCursor)
				if alloc-read < ringUsable {
					goto spaceAvailable
				}
			}
			return false
		}
		if alloc-read >= ringUsable-8192 {
			runtime.Gosched()
		}
	spaceAvailable:
		if !atomic.CompareAndSwapUint64(&s.allocCursor, alloc, alloc+1) {
			continue
		}

		idx := alloc & RingMask
		payload := &s.slots[idx]
		payload.ready.Store(0)
		payload.Priority = priority
		payload.Length = uint32(copy(payload.Data[:], data))
		payload.ready.Store(1)

		for {
			if atomic.LoadUint64(&s.writeCursor) == alloc {
				atomic.StoreUint64(&s.writeCursor, alloc+1)
				return true
			}
			runtime.Gosched()
		}
	}
}

type Config struct {
	LogDir                string
	FlushBufferSize       int
	RotateSize            int64
	RotateInterval        time.Duration
	DiskLatencyLimit      time.Duration
	PersistQueueDepth     int
	PersistEnqueueTimeout time.Duration
}

const (
	defaultAvgLogLineBytes   = 200
	minPersistQueueDepth     = 64
	maxPersistQueueDepth     = 4096
	defaultPersistEnqueueDur = 25 * time.Millisecond
)

func ComputePersistQueueDepth(cfg Config) int {
	if cfg.PersistQueueDepth > 0 {
		if cfg.PersistQueueDepth > maxPersistQueueDepth {
			return maxPersistQueueDepth
		}
		return cfg.PersistQueueDepth
	}
	flush := cfg.FlushBufferSize
	if flush <= 0 {
		flush = 256 * 1024
	}
	depth := (2 * flush / defaultAvgLogLineBytes) * 2
	if depth < minPersistQueueDepth {
		depth = minPersistQueueDepth
	}
	if depth > maxPersistQueueDepth {
		depth = maxPersistQueueDepth
	}
	return depth
}

type Logger struct {
	cfg                   Config
	shards                []*LogShard
	activeFile            *os.File
	fileOpenedAt          time.Time
	bytesWritten          int64
	diskDegraded          atomic.Int32
	loadSheddingEvents    atomic.Uint64
	persistQueueDrops     atomic.Uint64
	persistQueueDropBytes atomic.Uint64
	emaLatency            atomic.Uint64
	writerIndex           atomic.Uint64
	persistCh             chan *AlignedBuffer
	persistQueueCap       int
	wg                    sync.WaitGroup
	closeChan             chan struct{}

	cipherAEAD  cipher.AEAD
	encryptBuf  []byte
	compressBuf []byte
	nonceBuf    [12]byte
}

func deriveKeyFromEnv() ([]byte, error) {
	passphrase := os.Getenv("LOG_ENCRYPTION_KEY")
	if passphrase == "" {
		passphrase = "default-espx-logger-fallback-passphrase-change-me"
	}
	salt := []byte("espx-logger-salt-salt")
	return pbkdf2.Key([]byte(passphrase), salt, 4096, 32, sha256.New), nil
}

func (l *Logger) incrementNonce() {
	for i := len(l.nonceBuf) - 1; i >= 0; i-- {
		l.nonceBuf[i]++
		if l.nonceBuf[i] != 0 {
			break
		}
	}
}

func NewLogger(cfg Config, numShards int) *Logger {
	if cfg.PersistEnqueueTimeout <= 0 {
		cfg.PersistEnqueueTimeout = defaultPersistEnqueueDur
	}
	queueDepth := ComputePersistQueueDepth(cfg)
	shards := make([]*LogShard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = NewLogShard()
	}

	key, err := deriveKeyFromEnv()
	if err != nil {
		panic(err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		panic(err)
	}

	bufSize := cfg.FlushBufferSize
	if bufSize <= 0 {
		bufSize = 256 * 1024
	}
	encryptBuf := make([]byte, 4+12+bufSize+16+128)

	var nonceBuf [12]byte
	if _, err := rand.Read(nonceBuf[:]); err != nil {
		panic(err)
	}

	l := &Logger{
		cfg:             cfg,
		shards:          shards,
		persistCh:       make(chan *AlignedBuffer, queueDepth),
		persistQueueCap: queueDepth,
		closeChan:       make(chan struct{}),
		cipherAEAD:      aesgcm,
		encryptBuf:      encryptBuf,
		compressBuf:     make([]byte, 0, bufSize),
		nonceBuf:        nonceBuf,
	}
	_ = os.MkdirAll(l.cfg.LogDir, 0755)
	l.openActiveFile()
	l.wg.Add(4)
	go l.StartDrainer()
	go l.StartPersister()
	go l.StartDiskMonitor()
	go l.StartCompressorWorker()
	return l
}

func (l *Logger) StartCompressorWorker() {
	defer l.wg.Done()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-l.closeChan:
			l.processPendingSegments()
			return
		case <-ticker.C:
			l.processPendingSegments()
		}
	}
}

func (l *Logger) processPendingSegments() {
	files, err := os.ReadDir(l.cfg.LogDir)
	if err != nil {
		return
	}

	for _, f := range files {
		if f.IsDir() {
			continue
		}
		name := f.Name()
		if strings.HasPrefix(name, "segment_") && strings.HasSuffix(name, ".log") {
			srcPath := filepath.Join(l.cfg.LogDir, name)
			dstPath := filepath.Join(l.cfg.LogDir, name+".zst.ready")

			_ = l.compressAndEncryptFile(srcPath, dstPath)
		}
	}
}

func (l *Logger) compressAndEncryptFile(srcPath, dstPath string) error {
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	tmpPath := dstPath + ".tmp"
	dstFile, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer func() {
		_ = dstFile.Close()
		_ = os.Remove(tmpPath)
	}()

	bufSize := l.cfg.FlushBufferSize
	if bufSize <= 0 {
		bufSize = 256 * 1024
	}

	readBuf := make([]byte, bufSize)
	compressBuf := make([]byte, 0, bufSize)
	encryptBuf := make([]byte, 4+12+bufSize+16+128)

	enc := zstdEncoderPool.Get().(*zstd.Encoder)
	defer zstdEncoderPool.Put(enc)

	for {
		n, err := io.ReadFull(srcFile, readBuf)
		if n > 0 {
			data := readBuf[:n]
			compressBuf = enc.EncodeAll(data, compressBuf[:0])

			totalLen := 4 + 12 + len(compressBuf) + 16
			if len(encryptBuf) < totalLen {
				encryptBuf = make([]byte, totalLen+128)
			}

			l.incrementNonce()
			binary.BigEndian.PutUint32(encryptBuf[0:4], uint32(12+len(compressBuf)+16))
			copy(encryptBuf[4:16], l.nonceBuf[:])
			_ = l.cipherAEAD.Seal(encryptBuf[16:16], l.nonceBuf[:], compressBuf, nil)

			if _, err := dstFile.Write(encryptBuf[:totalLen]); err != nil {
				return err
			}
		}

		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return err
		}
	}

	if err := dstFile.Sync(); err != nil {
		return err
	}
	if err := dstFile.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, dstPath); err != nil {
		return err
	}

	_ = srcFile.Close()
	return os.Remove(srcPath)
}

func (l *Logger) Close() {
	close(l.closeChan)
	l.wg.Wait()
	if l.activeFile != nil {
		_ = l.activeFile.Close()
	}
}

func (l *Logger) Shards() []*LogShard {
	return l.shards
}

func (l *Logger) WriteToShard(shardID int, priority uint8, data []byte) bool {
	if shardID < 0 || shardID >= len(l.shards) {
		return false
	}
	return l.shards[shardID].Write(priority, data)
}

func (l *Logger) Write(priority uint8, data []byte) bool {
	shardID := int(l.writerIndex.Add(1) % uint64(len(l.shards)))
	return l.shards[shardID].Write(priority, data)
}

func DeriveKey(passphrase string) []byte {
	salt := []byte("espx-logger-salt-salt")
	return pbkdf2.Key([]byte(passphrase), salt, 4096, 32, sha256.New)
}

func DecryptSegment(filePath string, key []byte) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	var decrypted []byte
	header := make([]byte, 4)
	nonce := make([]byte, 12)

	for {
		_, err := io.ReadFull(file, header)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, err
		}

		length := binary.BigEndian.Uint32(header)
		if length < 12+16 {
			return nil, fmt.Errorf("invalid block length: %d", length)
		}

		if _, err := io.ReadFull(file, nonce); err != nil {
			return nil, err
		}

		ciphertextLen := length - 12
		ciphertext := make([]byte, ciphertextLen)
		if _, err := io.ReadFull(file, ciphertext); err != nil {
			return nil, err
		}

		plaintext, err := aesgcm.Open(nil, nonce, ciphertext, nil)
		if err != nil {
			return nil, err
		}

		decompressed, err := zstdDecoder.DecodeAll(plaintext, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress block: %w", err)
		}

		decrypted = append(decrypted, decompressed...)
	}

	return decrypted, nil
}
