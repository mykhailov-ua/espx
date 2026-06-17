package auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSlogMailer_AllTemplates ensures every notification template renders without error in the log mailer.
func TestSlogMailer_AllTemplates(t *testing.T) {
	mailer := SlogMailer{}
	ctx := context.Background()
	to := "advertiser@company.internal"

	t.Run("PasswordChanged", func(t *testing.T) {
		err := mailer.SendPasswordChangedEmail(ctx, to, "1.2.3.4", "Mozilla")
		assert.NoError(t, err)
	})

	t.Run("NewIPLogin", func(t *testing.T) {
		err := mailer.SendNewIPLoginEmail(ctx, to, "5.6.7.8", "Chrome")
		assert.NoError(t, err)
	})

	t.Run("AccountLocked", func(t *testing.T) {
		err := mailer.SendAccountLockedEmail(ctx, to, "5.6.7.8", "15 minutes")
		assert.NoError(t, err)
	})

	t.Run("2FACode", func(t *testing.T) {
		err := mailer.Send2FACodeEmail(ctx, to, "987654")
		assert.NoError(t, err)
	})

	t.Run("TopUpBalance", func(t *testing.T) {
		err := mailer.SendTopUpBalanceEmail(ctx, to, "1000.00", "USD")
		assert.NoError(t, err)
	})

	t.Run("LowBalanceAlert", func(t *testing.T) {
		err := mailer.SendLowBalanceAlertEmail(ctx, to, "5.50 USD", "2.5")
		assert.NoError(t, err)
	})

	t.Run("MonthlyInvoice", func(t *testing.T) {
		err := mailer.SendMonthlyInvoiceEmail(ctx, to, "May 2026", "2350.12 USD")
		assert.NoError(t, err)
	})

	t.Run("CampaignDepleted", func(t *testing.T) {
		err := mailer.SendCampaignDepletedEmail(ctx, to, "Summer Sale Campaign", "camp-uuid-12345")
		assert.NoError(t, err)
	})

	t.Run("WeeklyPerformance", func(t *testing.T) {
		err := mailer.SendWeeklyPerformanceEmail(ctx, to, "1240", "150000", "0.08")
		assert.NoError(t, err)
	})

	t.Run("CreativeModeration", func(t *testing.T) {
		err := mailer.SendCreativeModerationEmail(ctx, to, "creative-42", "REJECTED", "Violates political advertising policies")
		assert.NoError(t, err)
	})
}
