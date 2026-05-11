package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	ServerPort              string
	DBDSN                   string
	RedisAddr               string
	RedisPassword           string
	RedisStreamName         string
	RedisGroupName          string
	RedisConsumerID         string
	CHDSN                   string
	AuthServerPort          string
	TokenSymmetricKey       string
	MaxRequestBodySize      int64
	ClickAmount             float64
	ImpressionAmount        float64
	EventBatchSize          int
	EventFlushMs            int
	StatsFlushMs            int
	MaxWorkers              int
	CHMaxWorkers            int
	LogRetentionDays        int
	DBMaxConns              int
	DBMinConns              int
	WriteTimeoutMs          int
	ShutdownTimeoutMs       int
	IdempotencyTTLHrs       int
	RateLimitPerMin         int
	RateLimitWindowMs       int
	DuplicateTTLSec         int
	CHBatchSize             int
	CHFlushIntervalMs       int
	PartitionPreCreateDays  int
	RegistrySyncIntervalMs  int
	BudgetSyncIntervalMs    int
	HttpReadHeaderTimeoutMs int
	HttpReadTimeoutMs       int
	HttpWriteTimeoutMs      int
	HttpIdleTimeoutMs       int
	DefaultTokenDurationHrs int
	StreamMaxLen            int
	RetryInitialWaitMs      int
	RetryMaxWaitMs          int
	MaxRetries              int
	StreamMinIdleMs         int
	Argon2Memory            int
	Argon2Iterations        int
	Argon2Parallelism       int
}

func getEnvInt(key string, fallback int) int {
	if value, ok := os.LookupEnv(key); ok {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return fallback
}

func getEnvFloat(key string, fallback float64) float64 {
	if value, ok := os.LookupEnv(key); ok {
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return floatVal
		}
	}
	return fallback
}

func getEnvInt64(key string, fallback int64) int64 {
	if value, ok := os.LookupEnv(key); ok {
		if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
			return intVal
		}
	}
	return fallback
}

func Load() (*Config, error) {
	cfg := &Config{
		ServerPort:              os.Getenv("SERVER_PORT"),
		DBDSN:                   os.Getenv("DB_DSN"),
		RedisAddr:               os.Getenv("REDIS_ADDR"),
		RedisPassword:           os.Getenv("REDIS_PASSWORD"),
		RedisStreamName:         os.Getenv("REDIS_STREAM_NAME"),
		RedisGroupName:          os.Getenv("REDIS_GROUP_NAME"),
		RedisConsumerID:         os.Getenv("REDIS_CONSUMER_ID"),
		EventBatchSize:          getEnvInt("EVENT_BATCH_SIZE", 1000),
		EventFlushMs:            getEnvInt("EVENT_FLUSH_MS", 500),
		StatsFlushMs:            getEnvInt("STATS_FLUSH_MS", 5000),
		MaxWorkers:              getEnvInt("MAX_WORKERS", 16),
		CHMaxWorkers:            getEnvInt("CH_MAX_WORKERS", 1),
		LogRetentionDays:        getEnvInt("LOG_RETENTION_DAYS", 7),
		DBMaxConns:              getEnvInt("DB_MAX_CONNS", 16),
		DBMinConns:              getEnvInt("DB_MIN_CONNS", 2),
		WriteTimeoutMs:          getEnvInt("WRITE_TIMEOUT_MS", 5000),
		ShutdownTimeoutMs:       getEnvInt("SHUTDOWN_TIMEOUT_MS", 15000),
		IdempotencyTTLHrs:       getEnvInt("IDEMPOTENCY_TTL_HRS", 24),
		RateLimitPerMin:         getEnvInt("RATE_LIMIT_PER_MIN", 100),
		RateLimitWindowMs:       getEnvInt("RATE_LIMIT_WINDOW_MS", 60000),
		MaxRequestBodySize:      getEnvInt64("MAX_REQUEST_BODY_SIZE", 1048576),
		DuplicateTTLSec:         getEnvInt("DUPLICATE_TTL_SEC", 10),
		CHDSN:                   os.Getenv("CH_DSN"),
		CHBatchSize:             getEnvInt("CH_BATCH_SIZE", 50000),
		CHFlushIntervalMs:       getEnvInt("CH_FLUSH_INTERVAL_MS", 10000),
		AuthServerPort:          os.Getenv("AUTH_SERVER_PORT"),
		TokenSymmetricKey:       os.Getenv("TOKEN_SYMMETRIC_KEY"),
		PartitionPreCreateDays:  getEnvInt("PARTITION_PRECREATE_DAYS", 2),
		RegistrySyncIntervalMs:  getEnvInt("REGISTRY_SYNC_INTERVAL_MS", 60000),
		BudgetSyncIntervalMs:    getEnvInt("BUDGET_SYNC_INTERVAL_MS", 5000),
		HttpReadHeaderTimeoutMs: getEnvInt("HTTP_READ_HEADER_TIMEOUT_MS", 2000),
		HttpReadTimeoutMs:       getEnvInt("HTTP_READ_TIMEOUT_MS", 5000),
		HttpWriteTimeoutMs:      getEnvInt("HTTP_WRITE_TIMEOUT_MS", 10000),
		HttpIdleTimeoutMs:       getEnvInt("HTTP_IDLE_TIMEOUT_MS", 30000),
		DefaultTokenDurationHrs: getEnvInt("DEFAULT_TOKEN_DURATION_HRS", 24),
		ClickAmount:             getEnvFloat("CLICK_AMOUNT", 0.10),
		ImpressionAmount:        getEnvFloat("IMPRESSION_AMOUNT", 0.01),
		StreamMaxLen:            getEnvInt("STREAM_MAX_LEN", 100000),
		RetryInitialWaitMs:      getEnvInt("RETRY_INITIAL_WAIT_MS", 100),
		RetryMaxWaitMs:          getEnvInt("RETRY_MAX_WAIT_MS", 5000),
		MaxRetries:              getEnvInt("MAX_RETRIES", 5),
		StreamMinIdleMs:         getEnvInt("STREAM_MIN_IDLE_MS", 300000),
		Argon2Memory:            getEnvInt("ARGON2_MEMORY", 65536),
		Argon2Iterations:        getEnvInt("ARGON2_ITERATIONS", 3),
		Argon2Parallelism:       getEnvInt("ARGON2_PARALLELISM", 4),
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

	if cfg.AuthServerPort == "" {
		cfg.AuthServerPort = "50051"
	}
	if cfg.TokenSymmetricKey == "" {
		cfg.TokenSymmetricKey = "01234567890123456789012345678901"
	}

	return cfg, nil
}
