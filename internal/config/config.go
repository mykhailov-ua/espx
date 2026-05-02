package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	ServerPort        string
	DBDSN             string
	RedisAddr         string
	RedisPassword     string
	RedisStreamName   string
	RedisGroupName    string
	RedisConsumerID   string
	EventBatchSize    int
	EventFlushMs      int
	StatsFlushMs      int
	MaxWorkers        int
	LogRetentionDays  int
	DBMaxConns        int
	DBMinConns        int
	WriteTimeoutMs    int
	ShutdownTimeoutMs int
	RateLimitPerMin   int
	DuplicateTTLSec   int
	CHDSN             string
	CHBatchSize       int
	CHFlushIntervalMs int
}

func getEnvInt(key string, fallback int) int {
	if value, ok := os.LookupEnv(key); ok {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return fallback
}

func Load() (*Config, error) {
	cfg := &Config{
		ServerPort:        os.Getenv("SERVER_PORT"),
		DBDSN:             os.Getenv("DB_DSN"),
		RedisAddr:         os.Getenv("REDIS_ADDR"),
		RedisPassword:     os.Getenv("REDIS_PASSWORD"),
		RedisStreamName:   os.Getenv("REDIS_STREAM_NAME"),
		RedisGroupName:    os.Getenv("REDIS_GROUP_NAME"),
		RedisConsumerID:   os.Getenv("REDIS_CONSUMER_ID"),
		EventBatchSize:    getEnvInt("EVENT_BATCH_SIZE", 1000),
		EventFlushMs:      getEnvInt("EVENT_FLUSH_MS", 500),
		StatsFlushMs:      getEnvInt("STATS_FLUSH_MS", 5000),
		MaxWorkers:        getEnvInt("MAX_WORKERS", 16), // Set to 16
		LogRetentionDays:  getEnvInt("LOG_RETENTION_DAYS", 7),
		DBMaxConns:        getEnvInt("DB_MAX_CONNS", 16), // Match with workers
		DBMinConns:        getEnvInt("DB_MIN_CONNS", 2),
		WriteTimeoutMs:    getEnvInt("WRITE_TIMEOUT_MS", 5000),
		ShutdownTimeoutMs: getEnvInt("SHUTDOWN_TIMEOUT_MS", 15000),
		RateLimitPerMin:   getEnvInt("RATE_LIMIT_PER_MIN", 100),
		DuplicateTTLSec:   getEnvInt("DUPLICATE_TTL_SEC", 10),
		CHDSN:             os.Getenv("CH_DSN"),
		CHBatchSize:       getEnvInt("CH_BATCH_SIZE", 50000),
		CHFlushIntervalMs: getEnvInt("CH_FLUSH_INTERVAL_MS", 10000),
	}

	if cfg.ServerPort == "" {
		return nil, fmt.Errorf("SERVER_PORT is required")
	}
	if cfg.DBDSN == "" {
		return nil, fmt.Errorf("DB_DSN is required")
	}
	if cfg.RedisAddr == "" {
		return nil, fmt.Errorf("REDIS_ADDR is required")
	}

	if cfg.RedisStreamName == "" {
		cfg.RedisStreamName = "ad:events:stream"
	}
	if cfg.RedisGroupName == "" {
		cfg.RedisGroupName = "ad:processor:group"
	}
	if cfg.RedisConsumerID == "" {
		hostname, _ := os.Hostname()
		if hostname == "" {
			hostname = "unknown"
		}
		cfg.RedisConsumerID = fmt.Sprintf("%s:%d", hostname, os.Getpid())
	}

	return cfg, nil
}
