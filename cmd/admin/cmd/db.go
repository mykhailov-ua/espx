package cmd

import (
	"context"
	"fmt"
	"strings"

	adsdb "espx/internal/ads/db"
	"espx/internal/auth"
	authdb "espx/internal/auth/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/spf13/cobra"
)

var dbCmd = &cobra.Command{
	Use:   "db",
	Short: "Database utilities and seeding",
}

var seedCmd = &cobra.Command{
	Use:   "seed",
	Short: "Seed the database with realistic synthetic test data (100 customers, 100 users, 10 brands, 1000 campaigns)",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		hasher, err := auth.NewPasswordHasher(
			uint32(cfg.Argon2Memory),
			uint32(cfg.Argon2Iterations),
			uint8(cfg.Argon2Parallelism),
		)
		if err != nil {
			return err
		}
		precomputedHash, err := hasher.HashPassword("Password123!")
		if err != nil {
			return err
		}

		fmt.Println("Beginning database seeding within transaction...")
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback(ctx)
				fmt.Println("Seeding rolled back due to error.")
			}
		}()

		adsQueries := adsdb.New(tx)
		authQueries := authdb.New(tx)

		fmt.Println("Seeding 100 customers...")
		customerIDs := make([]uuid.UUID, 100)
		for i := 1; i <= 100; i++ {
			cID := uuid.New()
			customerIDs[i-1] = cID
			_, err = adsQueries.CreateCustomer(ctx, adsdb.CreateCustomerParams{
				ID:       pgtype.UUID{Bytes: cID, Valid: true},
				Name:     fmt.Sprintf("Advertiser Customer %d", i),
				Balance:  100_000_000_000,
				Currency: "USD",
			})
			if err != nil {
				return fmt.Errorf("failed to seed customer %d: %w", i, err)
			}
		}

		for i := 0; i < 20; i++ {
			_, err = adsQueries.UpdateCustomerOverdraft(ctx, adsdb.UpdateCustomerOverdraftParams{
				AllowedOverdraft: 5_000_000_000,
				ID:               pgtype.UUID{Bytes: customerIDs[i], Valid: true},
			})
			if err != nil {
				return err
			}
		}

		fmt.Println("Seeding 100 users...")
		for i := 1; i <= 100; i++ {
			role := "advertiser"
			if i <= 5 {
				role = "admin"
			}
			_, err = authQueries.CreateUser(ctx, authdb.CreateUserParams{
				Email:        fmt.Sprintf("user%d@test.com", i),
				PasswordHash: precomputedHash,
				Role:         role,
				CustomerID:   pgtype.UUID{Bytes: customerIDs[i-1], Valid: true},
			})
			if err != nil {
				return fmt.Errorf("failed to seed user %d: %w", i, err)
			}
		}

		fmt.Println("Seeding 10 advertiser brands...")
		brandIDs := make([]uuid.UUID, 10)
		for i := 1; i <= 10; i++ {
			bID := uuid.New()
			brandIDs[i-1] = bID
			_, err = adsQueries.CreateBrand(ctx, adsdb.CreateBrandParams{
				ID:         pgtype.UUID{Bytes: bID, Valid: true},
				CustomerID: pgtype.UUID{Bytes: customerIDs[i-1], Valid: true},
				Name:       fmt.Sprintf("Global Elite Brand %d", i),
			})
			if err != nil {
				return fmt.Errorf("failed to seed brand %d: %w", i, err)
			}
		}

		fmt.Println("Seeding 1000 campaigns...")
		countries := []string{"US", "GB", "CA", "UA", "DE", "FR", "JP"}
		pacingModes := []adsdb.PacingModeType{adsdb.PacingModeTypeASAP, adsdb.PacingModeTypeEVEN}

		for i := 1; i <= 1000; i++ {
			campID := uuid.New()
			custIdx := (i - 1) % 100
			cID := customerIDs[custIdx]

			brandID := pgtype.UUID{}
			brandFcapKey := ""
			if custIdx < 10 {
				brandID = pgtype.UUID{Bytes: brandIDs[custIdx], Valid: true}
				brandFcapKey = fmt.Sprintf("brand:fcap:%s", brandIDs[custIdx].String())
			}

			targetCountries := countries[0 : 1+(i%len(countries))]

			pacing := pacingModes[i%len(pacingModes)]
			budgetLimit := int64(10_000_000_000 + (i%5)*5_000_000_000)
			dailyBudget := int64(1_000_000_000 + (i%3)*1_000_000_000)

			_, err = adsQueries.CreateCampaign(ctx, adsdb.CreateCampaignParams{
				ID:              pgtype.UUID{Bytes: campID, Valid: true},
				Name:            fmt.Sprintf("Campaign Performance Campaign %d", i),
				BudgetLimit:     budgetLimit,
				Status:          adsdb.CampaignStatusTypeACTIVE,
				CustomerID:      pgtype.UUID{Bytes: cID, Valid: true},
				PacingMode:      pacing,
				DailyBudget:     dailyBudget,
				Timezone:        "UTC",
				FreqLimit:       pgtype.Int4{Int32: 10, Valid: true},
				FreqWindow:      pgtype.Int4{Int32: 86400, Valid: true},
				TargetCountries: targetCountries,
				BrandID:         brandID,
				BrandFcapKey:    brandFcapKey,
			})
			if err != nil {
				return fmt.Errorf("failed to seed campaign %d: %w", i, err)
			}
		}

		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		fmt.Println("Seeding completed")
		fmt.Printf("Seed Database stats:\n  Customers: %d\n  Users:     %d\n  Brands:    %d\n  Campaigns: %d\n",
			100, 100, 10, 1000)
		return nil
	},
}

var campaignCmd = &cobra.Command{
	Use:   "campaign",
	Short: "CRUD management for campaigns",
}

var listCampaignsCmd = &cobra.Command{
	Use:   "list",
	Short: "List active campaigns",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		limit, _ := cmd.Flags().GetInt("limit")

		rows, err := pool.Query(ctx, "SELECT id, name, status, budget_limit, current_spend, customer_id, pacing_mode FROM campaigns WHERE deleted_at IS NULL ORDER BY created_at DESC LIMIT $1", limit)
		if err != nil {
			return err
		}
		defer rows.Close()

		fmt.Printf("%-36s | %-35s | %-9s | %-12s | %-12s | %-36s\n", "ID", "Name", "Status", "Budget Limit", "Spend", "Customer ID")
		for rows.Next() {
			var id, customerID pgtype.UUID
			var name, status, pacing string
			var budgetLimit, currentSpend int64
			if err := rows.Scan(&id, &name, &status, &budgetLimit, &currentSpend, &customerID, &pacing); err != nil {
				return err
			}
			fmt.Printf("%-36s | %-35.35s | %-9s | %-12d | %-12d | %-36s\n",
				pgUUIDToGoogleUUID(id).String(), name, status, budgetLimit, currentSpend, pgUUIDToGoogleUUID(customerID).String())
		}
		return nil
	},
}

var getCampaignCmd = &cobra.Command{
	Use:   "get [id]",
	Short: "Get campaign details by ID",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		id, err := uuid.Parse(args[0])
		if err != nil {
			return err
		}

		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		queries := adsdb.New(pool)
		camp, err := queries.GetCampaign(ctx, pgtype.UUID{Bytes: id, Valid: true})
		if err != nil {
			return err
		}

		fmt.Printf("Campaign Details:\n")
		fmt.Printf("  ID:             %s\n", pgUUIDToGoogleUUID(camp.ID).String())
		fmt.Printf("  Name:           %s\n", camp.Name)
		fmt.Printf("  Status:         %s\n", camp.Status)
		fmt.Printf("  Budget Limit:   %d\n", camp.BudgetLimit)
		fmt.Printf("  Current Spend:  %d\n", camp.CurrentSpend)
		fmt.Printf("  Pacing Mode:    %s\n", camp.PacingMode)
		fmt.Printf("  Daily Budget:   %d\n", camp.DailyBudget)
		fmt.Printf("  Customer ID:    %s\n", pgUUIDToGoogleUUID(camp.CustomerID).String())
		fmt.Printf("  Target Nations: %s\n", strings.Join(camp.TargetCountries, ", "))
		if camp.BrandID.Valid {
			fmt.Printf("  Brand ID:       %s\n", pgUUIDToGoogleUUID(camp.BrandID).String())
			fmt.Printf("  Brand FCAP Key: %s\n", camp.BrandFcapKey)
		}
		return nil
	},
}

var createCampaignCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new campaign manually",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		custIDStr, _ := cmd.Flags().GetString("customer-id")
		limit, _ := cmd.Flags().GetInt64("budget-limit")
		daily, _ := cmd.Flags().GetInt64("daily-budget")
		pacing, _ := cmd.Flags().GetString("pacing")

		custID, err := uuid.Parse(custIDStr)
		if err != nil {
			return err
		}

		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		queries := adsdb.New(pool)
		camp, err := queries.CreateCampaign(ctx, adsdb.CreateCampaignParams{
			ID:          pgtype.UUID{Bytes: uuid.New(), Valid: true},
			Name:        name,
			CustomerID:  pgtype.UUID{Bytes: custID, Valid: true},
			BudgetLimit: limit,
			DailyBudget: daily,
			PacingMode:  adsdb.PacingModeType(pacing),
			Status:      adsdb.CampaignStatusTypeACTIVE,
			Timezone:    "UTC",
		})
		if err != nil {
			return err
		}

		fmt.Printf("Successfully created campaign:\n  ID:   %s\n  Name: %s\n  Cust: %s\n",
			pgUUIDToGoogleUUID(camp.ID).String(), camp.Name, pgUUIDToGoogleUUID(camp.CustomerID).String())
		return nil
	},
}

var deleteCampaignCmd = &cobra.Command{
	Use:   "delete [id]",
	Short: "Soft-delete a campaign",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		id, err := uuid.Parse(args[0])
		if err != nil {
			return err
		}

		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		queries := adsdb.New(pool)
		err = queries.SoftDeleteCampaign(ctx, pgtype.UUID{Bytes: id, Valid: true})
		if err != nil {
			return err
		}

		fmt.Printf("Successfully deleted campaign %s\n", id)
		return nil
	},
}

var customerCmd = &cobra.Command{
	Use:   "customer",
	Short: "CRUD management for customers",
}

var listCustomersCmd = &cobra.Command{
	Use:   "list",
	Short: "List all customers",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		queries := adsdb.New(pool)
		customers, err := queries.ListCustomers(ctx, adsdb.ListCustomersParams{Limit: 50, Offset: 0})
		if err != nil {
			return err
		}

		fmt.Printf("%-36s | %-25s | %-12s | %-12s | %-8s\n", "Customer ID", "Name", "Balance", "Overdraft", "Currency")
		for _, cust := range customers {
			fmt.Printf("%-36s | %-25.25s | %-12d | %-12d | %-8s\n",
				pgUUIDToGoogleUUID(cust.ID).String(), cust.Name, cust.Balance, cust.AllowedOverdraft, cust.Currency)
		}
		return nil
	},
}

var getCustomerCmd = &cobra.Command{
	Use:   "get [id]",
	Short: "Get customer details by ID",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		id, err := uuid.Parse(args[0])
		if err != nil {
			return err
		}

		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		queries := adsdb.New(pool)
		cust, err := queries.GetCustomerByID(ctx, pgtype.UUID{Bytes: id, Valid: true})
		if err != nil {
			return err
		}

		fmt.Printf("Customer Details:\n")
		fmt.Printf("  ID:                %s\n", pgUUIDToGoogleUUID(cust.ID).String())
		fmt.Printf("  Name:              %s\n", cust.Name)
		fmt.Printf("  Balance:           %d\n", cust.Balance)
		fmt.Printf("  Allowed Overdraft: %d\n", cust.AllowedOverdraft)
		fmt.Printf("  Currency:          %s\n", cust.Currency)
		fmt.Printf("  Created At:        %v\n", cust.CreatedAt.Time)
		return nil
	},
}

var createCustomerCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new customer manually",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		balance, _ := cmd.Flags().GetInt64("balance")
		overdraft, _ := cmd.Flags().GetInt64("allowed-overdraft")

		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		cID := uuid.New()

		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback(ctx)
			}
		}()

		txQueries := adsdb.New(tx)
		cust, err := txQueries.CreateCustomer(ctx, adsdb.CreateCustomerParams{
			ID:       pgtype.UUID{Bytes: cID, Valid: true},
			Name:     name,
			Balance:  balance,
			Currency: "USD",
		})
		if err != nil {
			return err
		}

		if overdraft > 0 {
			cust, err = txQueries.UpdateCustomerOverdraft(ctx, adsdb.UpdateCustomerOverdraftParams{
				AllowedOverdraft: overdraft,
				ID:               pgtype.UUID{Bytes: cID, Valid: true},
			})
			if err != nil {
				return err
			}
		}

		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		fmt.Printf("Successfully created customer:\n  ID:   %s\n  Name: %s\n  Bal:  %d\n",
			pgUUIDToGoogleUUID(cust.ID).String(), cust.Name, cust.Balance)
		return nil
	},
}

var updateCustomerCmd = &cobra.Command{
	Use:   "update [id]",
	Short: "Update customer balance/overdraft",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		cID, err := uuid.Parse(args[0])
		if err != nil {
			return err
		}

		balance, _ := cmd.Flags().GetInt64("balance")
		overdraft, _ := cmd.Flags().GetInt64("allowed-overdraft")

		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		if balance > 0 {
			_, err = pool.Exec(ctx, "UPDATE customers SET balance = $1, updated_at = NOW() WHERE id = $2", balance, pgtype.UUID{Bytes: cID, Valid: true})
			if err != nil {
				return err
			}
		}

		if overdraft > 0 {
			_, err = pool.Exec(ctx, "UPDATE customers SET allowed_overdraft = $1, updated_at = NOW() WHERE id = $2", overdraft, pgtype.UUID{Bytes: cID, Valid: true})
			if err != nil {
				return err
			}
		}

		fmt.Printf("Successfully updated customer %s\n", cID)
		return nil
	},
}

var deleteCustomerCmd = &cobra.Command{
	Use:   "delete [id]",
	Short: "Hard-delete a customer from PG",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		id, err := uuid.Parse(args[0])
		if err != nil {
			return err
		}

		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		res, err := pool.Exec(ctx, "DELETE FROM customers WHERE id = $1", pgtype.UUID{Bytes: id, Valid: true})
		if err != nil {
			return err
		}

		if res.RowsAffected() == 0 {
			return fmt.Errorf("customer %s not found", id)
		}

		fmt.Printf("Successfully deleted customer %s\n", id)
		return nil
	},
}

var blacklistCmd = &cobra.Command{
	Use:   "blacklist",
	Short: "Manage blocked IP addresses",
}

var listBlacklistCmd = &cobra.Command{
	Use:   "list",
	Short: "List all blacklisted IPs",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		queries := adsdb.New(pool)
		ips, err := queries.GetAllBlacklist(ctx)
		if err != nil {
			return err
		}

		fmt.Printf("%-18s | %-40s\n", "IP Address", "Blocked Reason")
		for _, ip := range ips {
			fmt.Printf("%-18s | %-40s\n", ip.Ip, ip.Reason)
		}
		return nil
	},
}

var addBlacklistCmd = &cobra.Command{
	Use:   "add",
	Short: "Add an IP to the blacklist",
	RunE: func(cmd *cobra.Command, args []string) error {
		ip, _ := cmd.Flags().GetString("ip")
		reason, _ := cmd.Flags().GetString("reason")

		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		queries := adsdb.New(pool)
		res, err := queries.CreateBlacklistIP(ctx, adsdb.CreateBlacklistIPParams{
			Ip:     ip,
			Reason: reason,
		})
		if err != nil {
			return err
		}

		fmt.Printf("Successfully added IP %s to blacklist. Reason: %s\n", res.Ip, res.Reason)
		return nil
	},
}

var deleteBlacklistCmd = &cobra.Command{
	Use:   "delete [ip]",
	Short: "Remove an IP from the blacklist",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ip := args[0]

		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		queries := adsdb.New(pool)
		err = queries.DeleteBlacklistIP(ctx, ip)
		if err != nil {
			return err
		}

		fmt.Printf("Successfully removed IP %s from blacklist\n", ip)
		return nil
	},
}

func init() {
	listCampaignsCmd.Flags().Int("limit", 20, "Number of campaigns to list")

	createCampaignCmd.Flags().String("name", "", "Name of the campaign")
	createCampaignCmd.Flags().String("customer-id", "", "Customer UUID")
	createCampaignCmd.Flags().Int64("budget-limit", 100_000_000, "Budget limit")
	createCampaignCmd.Flags().Int64("daily-budget", 10_000_000, "Daily budget limit")
	createCampaignCmd.Flags().String("pacing", "ASAP", "Pacing mode (ASAP/EVEN)")
	_ = createCampaignCmd.MarkFlagRequired("name")
	_ = createCampaignCmd.MarkFlagRequired("customer-id")

	campaignCmd.AddCommand(listCampaignsCmd)
	campaignCmd.AddCommand(getCampaignCmd)
	campaignCmd.AddCommand(createCampaignCmd)
	campaignCmd.AddCommand(deleteCampaignCmd)

	createCustomerCmd.Flags().String("name", "", "Customer's corporate name")
	createCustomerCmd.Flags().Int64("balance", 0, "Initial balance")
	createCustomerCmd.Flags().Int64("allowed-overdraft", 0, "Allowed overdraft limit")
	_ = createCustomerCmd.MarkFlagRequired("name")

	updateCustomerCmd.Flags().Int64("balance", 0, "Update balance amount")
	updateCustomerCmd.Flags().Int64("allowed-overdraft", 0, "Update allowed overdraft limit")

	customerCmd.AddCommand(listCustomersCmd)
	customerCmd.AddCommand(getCustomerCmd)
	customerCmd.AddCommand(createCustomerCmd)
	customerCmd.AddCommand(updateCustomerCmd)
	customerCmd.AddCommand(deleteCustomerCmd)

	addBlacklistCmd.Flags().String("ip", "", "IP address to blacklist")
	addBlacklistCmd.Flags().String("reason", "manual block via developer CLI", "Reason for blacklisting")
	_ = addBlacklistCmd.MarkFlagRequired("ip")

	blacklistCmd.AddCommand(listBlacklistCmd)
	blacklistCmd.AddCommand(addBlacklistCmd)
	blacklistCmd.AddCommand(deleteBlacklistCmd)

	dbCmd.AddCommand(seedCmd)

	rootCmd.AddCommand(dbCmd)
	rootCmd.AddCommand(campaignCmd)
	rootCmd.AddCommand(customerCmd)
	rootCmd.AddCommand(blacklistCmd)
}
