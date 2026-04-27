package config

import (
	"fmt"
	"os"
)

type Config struct {
	ServerPort string
	DBDSN      string
	RedisAddr  string
}

func Load() (*Config, error) {
	cfg := &Config{
		ServerPort: os.Getenv("SERVER_PORT"),
		DBDSN:      os.Getenv("DB_DSN"),
		RedisAddr:  os.Getenv("REDIS_ADDR"),
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

	return cfg, nil
}
