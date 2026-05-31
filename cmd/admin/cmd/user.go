package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth"
	authdb "github.com/mykhailov-ua/ad-event-processor/internal/auth/db"
	"github.com/spf13/cobra"
)

var userCmd = &cobra.Command{
	Use:   "user",
	Short: "Manage API users and generate access tokens",
}

var createTokenCmd = &cobra.Command{
	Use:   "create-token",
	Short: "Generate a PASETO token for API testing",
	RunE: func(cmd *cobra.Command, args []string) error {
		email, _ := cmd.Flags().GetString("email")
		autoCreate, _ := cmd.Flags().GetBool("auto-create")
		role, _ := cmd.Flags().GetString("role")

		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		repo := authdb.NewStore(pool)
		user, err := repo.GetUserByEmail(ctx, email)
		if err != nil {
			if autoCreate {
				fmt.Printf("User %s not found. Auto-creating...\n", email)

				hasher, err := auth.NewPasswordHasher(
					uint32(cfg.Argon2Memory),
					uint32(cfg.Argon2Iterations),
					uint8(cfg.Argon2Parallelism),
				)
				if err != nil {
					return err
				}
				hash, err := hasher.HashPassword("TestPass123!")
				if err != nil {
					return err
				}

				custID := uuid.New()
				res, err := repo.CreateUser(ctx, authdb.CreateUserParams{
					Email:        email,
					PasswordHash: hash,
					Role:         role,
					CustomerID:   pgtype.UUID{Bytes: custID, Valid: true},
				})
				if err != nil {
					return fmt.Errorf("failed to auto-create user: %w", err)
				}
				user = authdb.User{
					ID:         res.ID,
					Email:      res.Email,
					Role:       res.Role,
					CustomerID: res.CustomerID,
				}
				fmt.Printf("User created successfully with Customer ID: %s\n", custID.String())
			} else {
				return fmt.Errorf("user with email %s not found (use --auto-create to generate one)", email)
			}
		}

		tokenMaker, err := auth.NewPasetoMaker(string(cfg.TokenSymmetricKey))
		if err != nil {
			return err
		}

		userID := pgUUIDToGoogleUUID(user.ID)
		custID := pgUUIDToGoogleUUID(user.CustomerID)
		sessionID := uuid.New()
		duration := time.Duration(cfg.DefaultTokenDurationHrs) * time.Hour

		token, err := tokenMaker.CreateToken(userID, sessionID, user.Role, custID, duration)
		if err != nil {
			return fmt.Errorf("failed to generate token: %w", err)
		}

		fmt.Println("\nGenerated PASETO Token:")
		fmt.Println(token)
		fmt.Printf("Details:\n  User ID:     %s\n  Customer ID: %s\n  Role:        %s\n  Expires in:  %v\n",
			userID.String(), custID.String(), user.Role, duration)
		return nil
	},
}

var listUsersCmd = &cobra.Command{
	Use:   "list",
	Short: "List all users in the database",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		rows, err := pool.Query(ctx, "SELECT id, email, role, customer_id, is_blocked, email_verified FROM users ORDER BY created_at DESC")
		if err != nil {
			return err
		}
		defer rows.Close()

		fmt.Printf("%-36s | %-25s | %-10s | %-36s | %-7s | %-8s\n", "ID", "Email", "Role", "Customer ID", "Blocked", "Verified")
		for rows.Next() {
			var id, customerID pgtype.UUID
			var email, role string
			var isBlocked, emailVerified bool
			if err := rows.Scan(&id, &email, &role, &customerID, &isBlocked, &emailVerified); err != nil {
				return err
			}
			cIDStr := "NULL"
			if customerID.Valid {
				cIDStr = pgUUIDToGoogleUUID(customerID).String()
			}
			fmt.Printf("%-36s | %-25s | %-10s | %-36s | %-7t | %-8t\n",
				pgUUIDToGoogleUUID(id).String(), email, role, cIDStr, isBlocked, emailVerified)
		}
		return nil
	},
}

var getUserCmd = &cobra.Command{
	Use:   "get [email]",
	Short: "Get user details by email",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		email := args[0]
		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		repo := authdb.NewStore(pool)
		user, err := repo.GetUserByEmail(ctx, email)
		if err != nil {
			return fmt.Errorf("user not found: %w", err)
		}

		cIDStr := "NULL"
		if user.CustomerID.Valid {
			cIDStr = pgUUIDToGoogleUUID(user.CustomerID).String()
		}

		fmt.Printf("User Details:\n")
		fmt.Printf("  ID:             %s\n", pgUUIDToGoogleUUID(user.ID).String())
		fmt.Printf("  Email:          %s\n", user.Email)
		fmt.Printf("  Role:           %s\n", user.Role)
		fmt.Printf("  Customer ID:    %s\n", cIDStr)
		fmt.Printf("  Blocked:        %t\n", user.IsBlocked)
		fmt.Printf("  Email Verified: %t\n", user.EmailVerified)
		fmt.Printf("  Created At:     %v\n", user.CreatedAt.Time)
		fmt.Printf("  Updated At:     %v\n", user.UpdatedAt.Time)
		return nil
	},
}

var createUserCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new user manually",
	RunE: func(cmd *cobra.Command, args []string) error {
		email, _ := cmd.Flags().GetString("email")
		password, _ := cmd.Flags().GetString("password")
		role, _ := cmd.Flags().GetString("role")
		custIDStr, _ := cmd.Flags().GetString("customer-id")

		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		repo := authdb.NewStore(pool)

		hasher, err := auth.NewPasswordHasher(
			uint32(cfg.Argon2Memory),
			uint32(cfg.Argon2Iterations),
			uint8(cfg.Argon2Parallelism),
		)
		if err != nil {
			return err
		}
		hash, err := hasher.HashPassword(password)
		if err != nil {
			return err
		}

		var custID pgtype.UUID
		if custIDStr != "" {
			parsed, err := uuid.Parse(custIDStr)
			if err != nil {
				return fmt.Errorf("invalid customer-id UUID: %w", err)
			}
			custID = pgtype.UUID{Bytes: parsed, Valid: true}
		}

		res, err := repo.CreateUser(ctx, authdb.CreateUserParams{
			Email:        email,
			PasswordHash: hash,
			Role:         role,
			CustomerID:   custID,
		})
		if err != nil {
			return err
		}

		fmt.Printf("Successfully created user:\n  ID:    %s\n  Email: %s\n  Role:  %s\n",
			pgUUIDToGoogleUUID(res.ID).String(), res.Email, res.Role)
		return nil
	},
}

var updateUserCmd = &cobra.Command{
	Use:   "update [email]",
	Short: "Update an existing user's attributes",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		email := args[0]
		role, _ := cmd.Flags().GetString("role")
		block, _ := cmd.Flags().GetBool("block")
		unblock, _ := cmd.Flags().GetBool("unblock")
		custIDStr, _ := cmd.Flags().GetString("customer-id")

		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		var queryParts []string
		var queryArgs []any
		argIdx := 1

		if role != "" {
			queryParts = append(queryParts, fmt.Sprintf("role = $%d", argIdx))
			queryArgs = append(queryArgs, role)
			argIdx++
		}
		if block {
			queryParts = append(queryParts, fmt.Sprintf("is_blocked = $%d", argIdx))
			queryArgs = append(queryArgs, true)
			argIdx++
		} else if unblock {
			queryParts = append(queryParts, fmt.Sprintf("is_blocked = $%d", argIdx))
			queryArgs = append(queryArgs, false)
			argIdx++
		}
		if custIDStr != "" {
			parsed, err := uuid.Parse(custIDStr)
			if err != nil {
				return fmt.Errorf("invalid customer-id UUID: %w", err)
			}
			queryParts = append(queryParts, fmt.Sprintf("customer_id = $%d", argIdx))
			queryArgs = append(queryArgs, pgtype.UUID{Bytes: parsed, Valid: true})
			argIdx++
		}

		if len(queryParts) == 0 {
			return fmt.Errorf("no update attributes specified")
		}

		queryArgs = append(queryArgs, email)
		query := fmt.Sprintf("UPDATE users SET %s, updated_at = NOW() WHERE email = $%d", strings.Join(queryParts, ", "), argIdx)

		res, err := pool.Exec(ctx, query, queryArgs...)
		if err != nil {
			return err
		}

		if res.RowsAffected() == 0 {
			return fmt.Errorf("no user found with email %s", email)
		}

		fmt.Printf("Successfully updated user %s\n", email)
		return nil
	},
}

var deleteUserCmd = &cobra.Command{
	Use:   "delete [email]",
	Short: "Delete a user by email",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		email := args[0]
		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		res, err := pool.Exec(ctx, "DELETE FROM users WHERE email = $1", email)
		if err != nil {
			return err
		}

		if res.RowsAffected() == 0 {
			return fmt.Errorf("user with email %s not found", email)
		}

		fmt.Printf("Successfully deleted user %s\n", email)
		return nil
	},
}

func pgUUIDToGoogleUUID(p pgtype.UUID) uuid.UUID {
	if !p.Valid {
		return uuid.Nil
	}
	return p.Bytes
}

func init() {
	createTokenCmd.Flags().String("email", "", "User email address")
	createTokenCmd.Flags().Bool("auto-create", false, "Auto-create user if they do not exist")
	createTokenCmd.Flags().String("role", "admin", "Auto-created user's role (admin/advertiser/etc.)")
	_ = createTokenCmd.MarkFlagRequired("email")

	createUserCmd.Flags().String("email", "", "Email of new user")
	createUserCmd.Flags().String("password", "", "Password of new user")
	createUserCmd.Flags().String("role", "advertiser", "Role of new user")
	createUserCmd.Flags().String("customer-id", "", "Associated Customer UUID")
	_ = createUserCmd.MarkFlagRequired("email")
	_ = createUserCmd.MarkFlagRequired("password")

	updateUserCmd.Flags().String("role", "", "Update user role")
	updateUserCmd.Flags().Bool("block", false, "Block user")
	updateUserCmd.Flags().Bool("unblock", false, "Unblock user")
	updateUserCmd.Flags().String("customer-id", "", "Update customer ID mapping")

	userCmd.AddCommand(createTokenCmd)
	userCmd.AddCommand(listUsersCmd)
	userCmd.AddCommand(getUserCmd)
	userCmd.AddCommand(createUserCmd)
	userCmd.AddCommand(updateUserCmd)
	userCmd.AddCommand(deleteUserCmd)

	rootCmd.AddCommand(userCmd)
}
