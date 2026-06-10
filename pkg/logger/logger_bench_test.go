package logger

import (
	"testing"
	"time"
)

func BenchmarkLoggerWriteToShard(b *testing.B) {
	cfg := Config{
		LogDir:           b.TempDir(),
		FlushBufferSize:  256 * 1024,
		RotateSize:       512 * 1024 * 1024,
		RotateInterval:   time.Hour,
		DiskLatencyLimit: time.Second,
	}
	l := NewLogger(cfg, 1)
	defer l.Close()
	data := []byte("{\"level\":\"info\",\"msg\":\"click event successfully processed\",\"priority\":1}")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.WriteToShard(0, 1, data)
	}
}

func BenchmarkLogShardWriteMPSC(b *testing.B) {
	s := NewLogShard()
	l := &Logger{
		cfg:    Config{FlushBufferSize: 256 * 1024},
		shards: []*LogShard{s},
	}
	data := []byte("{\"level\":\"info\",\"msg\":\"click event successfully processed\",\"priority\":1}")
	stop := make(chan struct{})
	go func() {
		buf := NewAlignedBuffer(l.cfg.FlushBufferSize)
		for {
			select {
			case <-stop:
				return
			default:
			}
			buf, _ = l.drainShards(buf)
			buf.Reset()
		}
	}()
	defer close(stop)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if !s.Write(1, data) {
				b.Fatal("write failed")
			}
		}
	})
}

func BenchmarkLoggerWriteParallel(b *testing.B) {
	cfg := Config{
		LogDir:           b.TempDir(),
		FlushBufferSize:  256 * 1024,
		RotateSize:       512 * 1024 * 1024,
		RotateInterval:   time.Hour,
		DiskLatencyLimit: time.Second,
	}
	l := NewLogger(cfg, 4) // 4 shards
	defer l.Close()
	data := []byte("{\"level\":\"info\",\"msg\":\"click event successfully processed\",\"priority\":1}")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Write(1, data)
		}
	})
}

func BenchmarkWriteBufferEncryption(b *testing.B) {
	cfg := Config{
		LogDir:           b.TempDir(),
		FlushBufferSize:  256 * 1024,
		RotateSize:       512 * 1024 * 1024,
		RotateInterval:   time.Hour,
		DiskLatencyLimit: time.Second,
	}
	l := NewLogger(cfg, 1)
	defer l.Close()

	buf := NewAlignedBuffer(cfg.FlushBufferSize)
	data := []byte("{\"level\":\"info\",\"msg\":\"click event successfully processed\",\"priority\":1}")
	for buf.Available() >= len(data) {
		buf.Write(data)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		l.writeBuffer(buf)
		l.bytesWritten = 0
	}
}
