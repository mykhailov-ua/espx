package cmd

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"espx/internal/ads"
	"espx/internal/config"
	"espx/internal/database"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
)

var (
	envPath string
	cfg     *config.Config
	logger  *slog.Logger
)

var rootCmd = &cobra.Command{
	Use:   "admin",
	Short: "Internal developer CLI management utility",
	Long:  `High-performance Cobra utility for debugging budgets, generating PASETO tokens, seeding, and database CRUD.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
		slog.SetDefault(logger)

		if err := loadEnvFile(envPath); err != nil {
			return fmt.Errorf("failed to load env file: %w", err)
		}

		c, err := config.Load()
		if err != nil {
			return fmt.Errorf("failed to load configuration: %w", err)
		}
		cfg = c

		return nil
	},
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().StringVar(&envPath, "env-path", ".env", "path to .env configuration file")
}

func loadEnvFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		if (strings.HasPrefix(val, "\"") && strings.HasSuffix(val, "\"")) ||
			(strings.HasPrefix(val, "'") && strings.HasSuffix(val, "'")) {
			val = val[1 : len(val)-1]
		}
		if os.Getenv(key) == "" {
			_ = os.Setenv(key, val)
		}
	}
	return scanner.Err()
}

func getDB(ctx context.Context) (*pgxpool.Pool, error) {
	return database.Connect(ctx, string(cfg.DBDSN), 5, 1)
}

func getRedisShards(ctx context.Context) ([]redis.UniversalClient, *ads.JumpHashSharder, error) {
	var clients []redis.UniversalClient
	for _, addr := range cfg.RedisAddrs {
		rdb := redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs:    []string{addr},
			Password: string(cfg.RedisPassword),
			PoolSize: 10,
		})
		if err := rdb.Ping(ctx).Err(); err != nil {
			for _, c := range clients {
				_ = c.Close()
			}
			return nil, nil, fmt.Errorf("failed to ping redis shard at %s: %w", addr, err)
		}
		clients = append(clients, rdb)
	}

	sharder := ads.NewJumpHashSharder(len(clients))
	return clients, sharder, nil
}
