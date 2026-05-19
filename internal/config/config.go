package config

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/shopspring/decimal"
)

type Secret string

func (s Secret) LogValue() slog.Value {
	return slog.StringValue("**********")
}

type Config struct {
	ServerPort              string
	ProcessorPort           string
	ManagementPort          string
	DBDSN                   Secret
	RedisAddrs              []string
	RedisPassword           Secret
	RedisStreamName         string
	FraudStreamName         string
	RedisGroupName          string
	RedisConsumerID         string
	CHDSN                   Secret
	AuthServerPort          string
	AuthMetricsPort         string
	Env                     string
	TrustedProxies          []string
	TokenSymmetricKey       Secret
	MaxRequestBodySize      int64
	ClickAmount             decimal.Decimal
	ImpressionAmount        decimal.Decimal
	EventBatchSize          int
	EventFlushMs            int
	StatsFlushMs            int
	MaxWorkers              int
	CHMaxWorkers            int
	LogRetentionDays        int
	DBTrackerMaxConns       int
	DBProcessorMaxConns     int
	DBMinConns              int
	WriteTimeoutMs          int
	IdempotencyTTLHrs       int
	RateLimitPerMin         int
	RateLimitWindowMs       int
	DuplicateTTLSec         int
	TTCMinMs                int
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
	RedisPoolSize           int
	AdminAPIKey             Secret
	AllowedOrigins          []string
	Management              struct {
		RetentionDays          int
		CancellationFeePercent float64
	}
	CampaignUpdateChannel string

	Lifecycle struct {
		ShutdownTimeoutMs int
		DrainTimeoutMs    int
		WaitTimeoutMs     int
	}
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

func getEnvDecimal(key string, fallback decimal.Decimal) decimal.Decimal {
	if value, ok := os.LookupEnv(key); ok {
		if decVal, err := decimal.NewFromString(value); err == nil {
			return decVal
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
		ProcessorPort:           os.Getenv("PROCESSOR_PORT"),
		ManagementPort:          os.Getenv("MANAGEMENT_PORT"),
		DBDSN:                   Secret(os.Getenv("DB_DSN")),
		RedisAddrs:              strings.Split(os.Getenv("REDIS_ADDRS"), ","),
		RedisPassword:           Secret(os.Getenv("REDIS_PASSWORD")),
		RedisStreamName:         os.Getenv("REDIS_STREAM_NAME"),
		FraudStreamName:         os.Getenv("FRAUD_STREAM_NAME"),
		RedisGroupName:          os.Getenv("REDIS_GROUP_NAME"),
		RedisConsumerID:         os.Getenv("REDIS_CONSUMER_ID"),
		EventBatchSize:          getEnvInt("EVENT_BATCH_SIZE", 1000),
		EventFlushMs:            getEnvInt("EVENT_FLUSH_MS", 500),
		StatsFlushMs:            getEnvInt("STATS_FLUSH_MS", 5000),
		MaxWorkers:              getEnvInt("MAX_WORKERS", 16),
		CHMaxWorkers:            getEnvInt("CH_MAX_WORKERS", 1),
		LogRetentionDays:        getEnvInt("LOG_RETENTION_DAYS", 7),
		DBTrackerMaxConns:       getEnvInt("DB_TRACKER_MAX_CONNS", 4),
		DBProcessorMaxConns:     getEnvInt("DB_PROCESSOR_MAX_CONNS", 16),
		DBMinConns:              getEnvInt("DB_MIN_CONNS", 2),
		WriteTimeoutMs:          getEnvInt("WRITE_TIMEOUT_MS", 5000),
		IdempotencyTTLHrs:       getEnvInt("IDEMPOTENCY_TTL_HRS", 24),
		RateLimitPerMin:         getEnvInt("RATE_LIMIT_PER_MIN", 100),
		RateLimitWindowMs:       getEnvInt("RATE_LIMIT_WINDOW_MS", 60000),
		MaxRequestBodySize:      getEnvInt64("MAX_REQUEST_BODY_SIZE", 1048576),
		DuplicateTTLSec:         getEnvInt("DUPLICATE_TTL_SEC", 10),
		TTCMinMs:                getEnvInt("TTC_MIN_MS", 300),
		CHDSN:                   Secret(os.Getenv("CH_DSN")),
		CHBatchSize:             getEnvInt("CH_BATCH_SIZE", 50000),
		CHFlushIntervalMs:       getEnvInt("CH_FLUSH_INTERVAL_MS", 10000),
		AuthServerPort:          os.Getenv("AUTH_SERVER_PORT"),
		TokenSymmetricKey:       Secret(os.Getenv("TOKEN_SYMMETRIC_KEY")),
		PartitionPreCreateDays:  getEnvInt("PARTITION_PRECREATE_DAYS", 2),
		RegistrySyncIntervalMs:  getEnvInt("REGISTRY_SYNC_INTERVAL_MS", 60000),
		BudgetSyncIntervalMs:    getEnvInt("BUDGET_SYNC_INTERVAL_MS", 5000),
		HttpReadHeaderTimeoutMs: getEnvInt("HTTP_READ_HEADER_TIMEOUT_MS", 2000),
		HttpReadTimeoutMs:       getEnvInt("HTTP_READ_TIMEOUT_MS", 5000),
		HttpWriteTimeoutMs:      getEnvInt("HTTP_WRITE_TIMEOUT_MS", 10000),
		HttpIdleTimeoutMs:       getEnvInt("HTTP_IDLE_TIMEOUT_MS", 30000),
		DefaultTokenDurationHrs: getEnvInt("DEFAULT_TOKEN_DURATION_HRS", 24),
		ClickAmount:             getEnvDecimal("CLICK_AMOUNT", decimal.NewFromFloat(0.10)),
		ImpressionAmount:        getEnvDecimal("IMPRESSION_AMOUNT", decimal.NewFromFloat(0.01)),
		StreamMaxLen:            getEnvInt("STREAM_MAX_LEN", 100000),
		RetryInitialWaitMs:      getEnvInt("RETRY_INITIAL_WAIT_MS", 100),
		RetryMaxWaitMs:          getEnvInt("RETRY_MAX_WAIT_MS", 5000),
		MaxRetries:              getEnvInt("MAX_RETRIES", 5),
		StreamMinIdleMs:         getEnvInt("STREAM_MIN_IDLE_MS", 300000),
		Argon2Memory:            getEnvInt("ARGON2_MEMORY", 65536),
		Argon2Iterations:        getEnvInt("ARGON2_ITERATIONS", 3),
		Argon2Parallelism:       getEnvInt("ARGON2_PARALLELISM", 4),
		RedisPoolSize:           getEnvInt("REDIS_POOL_SIZE", 0),
		AdminAPIKey:             Secret(os.Getenv("ADMIN_API_KEY")),
		AllowedOrigins:          strings.Split(os.Getenv("ALLOWED_ORIGINS"), ","),
		TrustedProxies:          strings.Split(os.Getenv("TRUSTED_PROXIES"), ","),
		Env:                     os.Getenv("ENV"),
		AuthMetricsPort:         os.Getenv("AUTH_METRICS_PORT"),
		CampaignUpdateChannel:   os.Getenv("CAMPAIGN_UPDATE_CHANNEL"),
	}

	if len(cfg.AllowedOrigins) == 1 && cfg.AllowedOrigins[0] == "" {
		cfg.AllowedOrigins = []string{"https://dashboard.example.com", "http://localhost:8188"}
	}

	cfg.Management.RetentionDays = getEnvInt("MANAGEMENT_RETENTION_DAYS", 90)
	cfg.Management.CancellationFeePercent = getEnvFloat("MANAGEMENT_CANCELLATION_FEE_PERCENT", 5.0)

	cfg.Lifecycle.ShutdownTimeoutMs = getEnvInt("SHUTDOWN_TIMEOUT_MS", 15000)
	cfg.Lifecycle.DrainTimeoutMs = getEnvInt("DRAIN_TIMEOUT_MS", 10000)
	cfg.Lifecycle.WaitTimeoutMs = getEnvInt("WAIT_TIMEOUT_MS", 5000)

	if cfg.ServerPort == "" {
		return nil, errors.New("SERVER_PORT is required")
	}
	if cfg.ProcessorPort == "" {
		cfg.ProcessorPort = "8186"
	}
	if cfg.ManagementPort == "" {
		cfg.ManagementPort = "8188"
	}
	if cfg.DBDSN == "" {
		return nil, errors.New("DB_DSN is required")
	}
	if len(cfg.RedisAddrs) == 0 || cfg.RedisAddrs[0] == "" {
		return nil, errors.New("REDIS_ADDRS is required")
	}

	if cfg.RedisStreamName == "" {
		cfg.RedisStreamName = "ad:events:stream"
	}
	if cfg.FraudStreamName == "" {
		cfg.FraudStreamName = "ad:fraud:stream"
	}
	if cfg.RedisGroupName == "" {
		cfg.RedisGroupName = "ad:processor:group"
	}
	if cfg.RedisConsumerID == "" {
		hostname, _ := os.Hostname()
		if hostname == "" {
			hostname = "unknown"
		}
		cfg.RedisConsumerID = hostname + ":" + strconv.Itoa(os.Getpid())
	}

	if cfg.AuthServerPort == "" {
		cfg.AuthServerPort = "51051"
	}
	if cfg.AuthMetricsPort == "" {
		cfg.AuthMetricsPort = "9091"
	}
	if cfg.Env == "" {
		cfg.Env = "development"
	}
	if cfg.TokenSymmetricKey == "" {
		return nil, errors.New("TOKEN_SYMMETRIC_KEY is required")
	}

	return cfg, nil
}
