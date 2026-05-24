package management

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/database"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEdge_RoundingAndSmallAmounts(t *testing.T) {
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{}
	cfg.Management.CancellationFeePercent = 10.0
	cfg.Lifecycle.WaitTimeoutMs = 1
	svc := NewService(pool, []redis.UniversalClient{rdb}, ads.NewJumpHashSharder(1), cfg)
	defer svc.Close()


	customerID := uuid.New()
	_ = svc.CreateCustomer(context.Background(), customerID, "Small Saver", 100_000_000, "USD")

	budget := int64(1_050_000)
	id, err := svc.CreateCampaign(context.Background(), customerID, nil, "Tiny Camp", budget, db.PacingModeTypeASAP, 0, "UTC", 0, 0, nil, "idemp-1")
	require.NoError(t, err)

	err = svc.CancelCampaign(context.Background(), id, "Too small")
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		var finalBalance int64
		_ = pool.QueryRow(context.Background(), "SELECT balance FROM customers WHERE id = $1", customerID).Scan(&finalBalance)
		return finalBalance == 99895000
	}, 2*time.Second, 20*time.Millisecond)
}

func TestEdge_ConcurrentBalanceDepletion(t *testing.T) {
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	svc := NewService(pool, []redis.UniversalClient{rdb}, ads.NewJumpHashSharder(1), &config.Config{})
	defer svc.Close()

	customerID := uuid.New()
	_ = svc.CreateCustomer(context.Background(), customerID, "Poor db.User", 500_000_000, "USD")

	const workers = 10
	campaignBudget := int64(100_000_000)

	var wg sync.WaitGroup
	wg.Add(workers)
	results := make(chan error, workers)

	for i := 0; i < workers; i++ {
		go func(idx int) {
			defer wg.Done()
			_, err := svc.CreateCampaign(context.Background(), customerID, nil, fmt.Sprintf("Camp-%d", idx), campaignBudget, db.PacingModeTypeASAP, 0, "UTC", 0, 0, nil, fmt.Sprintf("idemp-%d", idx))
			results <- err
		}(i)
	}

	wg.Wait()
	close(results)

	var successCount, failureCount int
	for err := range results {
		if err == nil {
			successCount++
		} else {
			failureCount++
		}
	}

	assert.Equal(t, 5, successCount)
	assert.Equal(t, 5, failureCount)
}

func TestEdge_ResumingStuckSettlement(t *testing.T) {
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{}
	cfg.Management.CancellationFeePercent = 10
	cfg.Lifecycle.WaitTimeoutMs = 1
	svc := NewService(pool, []redis.UniversalClient{rdb}, ads.NewJumpHashSharder(1), cfg)
	defer svc.Close()

	customerID := uuid.New()
	_ = svc.CreateCustomer(context.Background(), customerID, "Crash Test", 1_000_000_000, "USD")
	campaignID, _ := svc.CreateCampaign(context.Background(), customerID, nil, "Zombie", 500_000_000, db.PacingModeTypeASAP, 0, "UTC", 0, 0, nil, "idemp-crash")

	_, _ = pool.Exec(context.Background(), "UPDATE campaigns SET status = 'DRAINING' WHERE id = $1", campaignID)

	err := svc.CancelCampaign(context.Background(), campaignID, "Resume")
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		var status string
		_ = pool.QueryRow(context.Background(), "SELECT status FROM campaigns WHERE id = $1", campaignID).Scan(&status)
		return status == "DELETED"
	}, 2*time.Second, 20*time.Millisecond)
}
