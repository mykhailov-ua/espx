package cmd

import (
	"context"
	"fmt"

	adsdb "espx/internal/ads/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/spf13/cobra"
)

var budgetCmd = &cobra.Command{
	Use:   "budget",
	Short: "Manage and reset campaign budget caches",
}

var budgetResetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Reset the Redis budget cache and spend limits for a campaign",
	RunE: func(cmd *cobra.Command, args []string) error {
		campIDStr, _ := cmd.Flags().GetString("campaign-id")
		resetSpend, _ := cmd.Flags().GetBool("reset-db-spend")

		campaignID, err := uuid.Parse(campIDStr)
		if err != nil {
			return fmt.Errorf("invalid campaign-id UUID: %w", err)
		}

		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		queries := adsdb.New(pool)
		camp, err := queries.GetCampaign(ctx, pgtype.UUID{Bytes: campaignID, Valid: true})
		if err != nil {
			return fmt.Errorf("campaign not found in database: %w", err)
		}

		_ = pgUUIDToGoogleUUID(camp.CustomerID)

		redisClients, sharder, err := getRedisShards(ctx)
		if err != nil {
			return err
		}
		defer func() {
			for _, rdb := range redisClients {
				_ = rdb.Close()
			}
		}()

		shardIdx := sharder.GetShard(campaignID)
		rdb := redisClients[shardIdx]

		fmt.Printf("Campaign %s maps to Redis Shard %d/%d\n", campaignID, shardIdx, len(redisClients))

		budgetKey := fmt.Sprintf("budget:campaign:%s", campaignID)
		syncKey := fmt.Sprintf("budget:sync:campaign:%s", campaignID)

		res1, err := rdb.Del(ctx, budgetKey).Result()
		if err != nil {
			return fmt.Errorf("failed to delete remaining budget cache: %w", err)
		}

		res2, err := rdb.Del(ctx, syncKey).Result()
		if err != nil {
			return fmt.Errorf("failed to delete campaign sync accumulator: %w", err)
		}

		res3, err := rdb.SRem(ctx, "budget:dirty_campaigns", campaignID.String()).Result()
		if err != nil {
			return fmt.Errorf("failed to remove campaign from dirty set: %w", err)
		}

		fmt.Printf("Cleared Redis cache:\n  DEL %s (%d)\n  DEL %s (%d)\n  SREM budget:dirty_campaigns %s (%d)\n",
			budgetKey, res1, syncKey, res2, campaignID, res3)

		if resetSpend {
			fmt.Println("Resetting database current_spend to 0...")
			_, err = pool.Exec(ctx, "UPDATE campaigns SET current_spend = 0, status = 'ACTIVE', updated_at = NOW() WHERE id = $1", pgtype.UUID{Bytes: campaignID, Valid: true})
			if err != nil {
				return fmt.Errorf("failed to update current_spend in DB: %w", err)
			}
			fmt.Println("PostgreSQL campaign current_spend successfully reset to 0, status set to ACTIVE.")
		}

		fmt.Println("Budget reset execution complete.")
		return nil
	},
}

func init() {
	budgetResetCmd.Flags().String("campaign-id", "", "UUID of the campaign to reset")
	budgetResetCmd.Flags().Bool("reset-db-spend", false, "Reset current_spend to 0 and set status to ACTIVE in PostgreSQL")
	_ = budgetResetCmd.MarkFlagRequired("campaign-id")

	budgetCmd.AddCommand(budgetResetCmd)
	rootCmd.AddCommand(budgetCmd)
}
