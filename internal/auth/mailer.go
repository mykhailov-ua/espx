package auth

import (
	"bytes"
	"context"
	"html/template"
	"log/slog"
	"time"
)

// Mailer delivers user-facing security, billing, and operational notifications without coupling auth to a specific email provider.
type Mailer interface {
	SendPasswordChangedEmail(ctx context.Context, toEmail, clientIP, userAgent string) error

	SendNewIPLoginEmail(ctx context.Context, toEmail, clientIP, userAgent string) error
	SendAccountLockedEmail(ctx context.Context, toEmail, clientIP, lockDuration string) error
	Send2FACodeEmail(ctx context.Context, toEmail, code string) error

	SendTopUpBalanceEmail(ctx context.Context, toEmail, amount, currency string) error
	SendLowBalanceAlertEmail(ctx context.Context, toEmail, currentBalance, remainingHours string) error
	SendMonthlyInvoiceEmail(ctx context.Context, toEmail, period, amount string) error

	SendCampaignDepletedEmail(ctx context.Context, toEmail, campaignName, campaignID string) error
	SendWeeklyPerformanceEmail(ctx context.Context, toEmail, clicks, impressions, ctr string) error
	SendCreativeModerationEmail(ctx context.Context, toEmail, creativeID, status, reason string) error
}

// SlogMailer implements Mailer by rendering HTML templates and logging dispatch for local and test environments.
type SlogMailer struct{}

// renderAndLog keeps local and CI environments observable without requiring an SMTP backend.
func renderAndLog(ctx context.Context, templateName string, tpl *template.Template, toEmail, subject string, data any) error {
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, data); err != nil {
		slog.Error("failed to render email template", slog.String("template", templateName), slog.Any("error", err))
		return err
	}

	snippetLen := 100
	if buf.Len() < 100 {
		snippetLen = buf.Len()
	}

	slog.Info("security_notification_dispatch",
		slog.String("channel", "email"),
		slog.String("recipient", toEmail),
		slog.String("subject", subject),
		slog.Int("rendered_bytes", buf.Len()),
		slog.String("rendered_snippet", buf.String()[:snippetLen]+"..."),
	)
	return nil
}

// passwordChangedHTMLTemplate is the HTML body for password change security alerts.
var passwordChangedHTMLTemplate = template.Must(template.New("password_changed").Parse(`<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Security Alert: Password Changed</title>
</head>
<body style="font-family: sans-serif; background-color: #f9fafb; color: #111827; margin: 0; padding: 40px 20px;">
    <div style="max-width: 576px; background-color: #ffffff; padding: 32px; border-radius: 12px; margin: auto; border: 1px solid #e5e7eb;">
        <div style="font-size: 20px; font-weight: 700; color: #dc2626; border-bottom: 2px solid #f3f4f6; padding-bottom: 16px; margin-bottom: 24px;">
            Security Alert: Password Changed
        </div>
        <p>Hello,</p>
        <p>The password for your account (<strong>{{.Email}}</strong>) was changed on <strong>{{.Time}}</strong>.</p>
        <div style="background-color: #f9fafb; border: 1px solid #f3f4f6; padding: 16px; margin: 20px 0; border-radius: 8px;">
            <strong>Request Details:</strong><br>
            IP: {{.IP}}<br>
            UA: {{.UserAgent}}
        </div>
        <p style="color: #dc2626; font-weight: 600;">If you did not request this, contact security immediately.</p>
    </div>
</body>
</html>`))

// newIPLoginHTMLTemplate is the HTML body for unfamiliar login location alerts.
var newIPLoginHTMLTemplate = template.Must(template.New("new_ip_login").Parse(`<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Security Alert: Login from New IP</title>
</head>
<body style="font-family: sans-serif; background-color: #f9fafb; color: #111827; margin: 0; padding: 40px 20px;">
    <div style="max-width: 576px; background-color: #ffffff; padding: 32px; border-radius: 12px; margin: auto; border: 1px solid #e5e7eb;">
        <div style="font-size: 20px; font-weight: 700; color: #d97706; border-bottom: 2px solid #f3f4f6; padding-bottom: 16px; margin-bottom: 24px;">
            Security Alert: Login from Unfamiliar IP
        </div>
        <p>Hello,</p>
        <p>We detected a new login to your account (<strong>{{.Email}}</strong>) from an unfamiliar IP address.</p>
        <div style="background-color: #f9fafb; border: 1px solid #f3f4f6; padding: 16px; margin: 20px 0; border-radius: 8px;">
            <strong>Login Details:</strong><br>
            IP Address: {{.IP}}<br>
            User Agent: {{.UserAgent}}<br>
            Time: {{.Time}}
        </div>
        <p>If this was you, no action is needed. If this looks suspicious, please change your password and revoke your sessions immediately.</p>
    </div>
</body>
</html>`))

// accountLockedHTMLTemplate is the HTML body for temporary account lockout notices.
var accountLockedHTMLTemplate = template.Must(template.New("account_locked").Parse(`<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Security Alert: Account Temporarily Locked</title>
</head>
<body style="font-family: sans-serif; background-color: #f9fafb; color: #111827; margin: 0; padding: 40px 20px;">
    <div style="max-width: 576px; background-color: #ffffff; padding: 32px; border-radius: 12px; margin: auto; border: 1px solid #e5e7eb;">
        <div style="font-size: 20px; font-weight: 700; color: #dc2626; border-bottom: 2px solid #f3f4f6; padding-bottom: 16px; margin-bottom: 24px;">
            Security Alert: Account Temporarily Locked
        </div>
        <p>Hello,</p>
        <p>Your account (<strong>{{.Email}}</strong>) has been temporarily locked due to too many failed login attempts.</p>
        <div style="background-color: #fef2f2; border: 1px solid #fee2e2; padding: 16px; margin: 20px 0; border-radius: 8px; color: #991b1b;">
            <strong>Lockout Details:</strong><br>
            Lock Duration: {{.LockDuration}}<br>
            Trigger IP: {{.IP}}<br>
            Time: {{.Time}}
        </div>
        <p>This lockout is automated and helps prevent brute force credential probing. You can try logging in again after the lockout period expires.</p>
    </div>
</body>
</html>`))

// twoFactorCodeHTMLTemplate is the HTML body for two-factor verification codes.
var twoFactorCodeHTMLTemplate = template.Must(template.New("two_factor_code").Parse(`<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Your Two-Factor Verification Code</title>
</head>
<body style="font-family: sans-serif; background-color: #f9fafb; color: #111827; margin: 0; padding: 40px 20px;">
    <div style="max-width: 576px; background-color: #ffffff; padding: 32px; border-radius: 12px; margin: auto; border: 1px solid #e5e7eb; text-align: center;">
        <div style="font-size: 20px; font-weight: 700; color: #2563eb; border-bottom: 2px solid #f3f4f6; padding-bottom: 16px; margin-bottom: 24px;">
            Two-Factor Verification Code
        </div>
        <p style="text-align: left;">Hello,</p>
        <p style="text-align: left;">Use the following security code to complete your sign-in process. This code is valid for 5 minutes.</p>
        <div style="font-size: 32px; font-weight: 800; letter-spacing: 0.1em; color: #1e40af; background-color: #eff6ff; padding: 20px; margin: 24px auto; border-radius: 8px; width: fit-content; border: 1px solid #bfdbfe;">
            {{.Code}}
        </div>
        <p style="text-align: left; font-size: 13px; color: #6b7280;">If you did not initiate this login attempt, please change your password immediately as your credentials might be compromised.</p>
    </div>
</body>
</html>`))

// topUpBalanceHTMLTemplate is the HTML body for successful balance top-up confirmations.
var topUpBalanceHTMLTemplate = template.Must(template.New("topup_balance").Parse(`<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Billing: Balance Top-Up Successful</title>
</head>
<body style="font-family: sans-serif; background-color: #f9fafb; color: #111827; margin: 0; padding: 40px 20px;">
    <div style="max-width: 576px; background-color: #ffffff; padding: 32px; border-radius: 12px; margin: auto; border: 1px solid #e5e7eb;">
        <div style="font-size: 20px; font-weight: 700; color: #16a34a; border-bottom: 2px solid #f3f4f6; padding-bottom: 16px; margin-bottom: 24px;">
            Balance Top-Up Successful
        </div>
        <p>Hello,</p>
        <p>We have successfully credited your balance top-up.</p>
        <div style="background-color: #f0fdf4; border: 1px solid #dcfce7; padding: 16px; margin: 20px 0; border-radius: 8px; color: #14532d; font-size: 18px; font-weight: 700;">
            Credited Amount: {{.Amount}} {{.Currency}}
        </div>
        <p>Your campaign limits have been updated and synchronized with our edge nodes. Thank you for advertising with us!</p>
    </div>
</body>
</html>`))

// lowBalanceAlertHTMLTemplate is the HTML body for low balance warnings before delivery stops.
var lowBalanceAlertHTMLTemplate = template.Must(template.New("low_balance_alert").Parse(`<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Billing Alert: Low Balance Notice</title>
</head>
<body style="font-family: sans-serif; background-color: #f9fafb; color: #111827; margin: 0; padding: 40px 20px;">
    <div style="max-width: 576px; background-color: #ffffff; padding: 32px; border-radius: 12px; margin: auto; border: 1px solid #e5e7eb;">
        <div style="font-size: 20px; font-weight: 700; color: #ea580c; border-bottom: 2px solid #f3f4f6; padding-bottom: 16px; margin-bottom: 24px;">
            Urgent: Low Account Balance
        </div>
        <p>Hello,</p>
        <p>Your account balance is running low. Please top up soon to prevent any disruption in ad delivery.</p>
        <div style="background-color: #fff7ed; border: 1px solid #ffedd5; padding: 16px; margin: 20px 0; border-radius: 8px; color: #7c2d12;">
            Current Balance: <strong>{{.CurrentBalance}}</strong><br>
            Estimated Run Time Remaining: <strong>{{.RemainingHours}} hours</strong>
        </div>
        <p>Once your balance is depleted, all active campaigns will automatically transition to suspended status in our sharded edge pool.</p>
    </div>
</body>
</html>`))

// monthlyInvoiceHTMLTemplate is the HTML body for monthly billing statement notices.
var monthlyInvoiceHTMLTemplate = template.Must(template.New("monthly_invoice").Parse(`<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Billing: Monthly Statement & Invoice Available</title>
</head>
<body style="font-family: sans-serif; background-color: #f9fafb; color: #111827; margin: 0; padding: 40px 20px;">
    <div style="max-width: 576px; background-color: #ffffff; padding: 32px; border-radius: 12px; margin: auto; border: 1px solid #e5e7eb;">
        <div style="font-size: 20px; font-weight: 700; color: #1e3a8a; border-bottom: 2px solid #f3f4f6; padding-bottom: 16px; margin-bottom: 24px;">
            Monthly Statement Available
        </div>
        <p>Hello,</p>
        <p>Your monthly billing invoice is ready for review.</p>
        <div style="background-color: #f8fafc; border: 1px solid #e2e8f0; padding: 16px; margin: 20px 0; border-radius: 8px;">
            Statement Period: <strong>{{.Period}}</strong><br>
            Total Spent: <strong>{{.Amount}}</strong>
        </div>
        <p>The detailed PDF ledger can be downloaded directly from your customer dashboard under Billing Settings.</p>
    </div>
</body>
</html>`))

// campaignDepletedHTMLTemplate is the HTML body for campaign budget exhaustion alerts.
var campaignDepletedHTMLTemplate = template.Must(template.New("campaign_depleted").Parse(`<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Operational Notification: Campaign Budget Depleted</title>
</head>
<body style="font-family: sans-serif; background-color: #f9fafb; color: #111827; margin: 0; padding: 40px 20px;">
    <div style="max-width: 576px; background-color: #ffffff; padding: 32px; border-radius: 12px; margin: auto; border: 1px solid #e5e7eb;">
        <div style="font-size: 20px; font-weight: 700; color: #6b7280; border-bottom: 2px solid #f3f4f6; padding-bottom: 16px; margin-bottom: 24px;">
            Campaign Budget Depleted
        </div>
        <p>Hello,</p>
        <p>Your ad campaign has reached its configured budget limit and is now suspended.</p>
        <div style="background-color: #f3f4f6; border: 1px solid #e5e7eb; padding: 16px; margin: 20px 0; border-radius: 8px;">
            Campaign Name: <strong>{{.CampaignName}}</strong><br>
            Campaign ID: <code>{{.CampaignID}}</code>
        </div>
        <p>To resume this campaign, please allocate more funds or update the budget limit in your campaign settings panel.</p>
    </div>
</body>
</html>`))

// weeklyPerformanceHTMLTemplate is the HTML body for weekly campaign performance summaries.
var weeklyPerformanceHTMLTemplate = template.Must(template.New("weekly_performance").Parse(`<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Operational Statement: Weekly Analytics Performance Report</title>
</head>
<body style="font-family: sans-serif; background-color: #f9fafb; color: #111827; margin: 0; padding: 40px 20px;">
    <div style="max-width: 576px; background-color: #ffffff; padding: 32px; border-radius: 12px; margin: auto; border: 1px solid #e5e7eb;">
        <div style="font-size: 20px; font-weight: 700; color: #0f766e; border-bottom: 2px solid #f3f4f6; padding-bottom: 16px; margin-bottom: 24px;">
            Weekly Analytics Summary
        </div>
        <p>Hello,</p>
        <p>Here is your weekly campaign telemetry and performance report:</p>
        <div style="background-color: #f0fdfa; border: 1px solid #ccfbf1; padding: 20px; margin: 20px 0; border-radius: 8px;">
            <table style="width: 100%; border-collapse: collapse;">
                <tr>
                    <td style="padding: 6px 0; color: #4f4f4f;">Total Impressions:</td>
                    <td style="padding: 6px 0; font-weight: 700; text-align: right;">{{.Impressions}}</td>
                </tr>
                <tr>
                    <td style="padding: 6px 0; color: #4f4f4f;">Total Clicks:</td>
                    <td style="padding: 6px 0; font-weight: 700; text-align: right;">{{.Clicks}}</td>
                </tr>
                <tr>
                    <td style="padding: 6px 0; color: #4f4f4f;">Average CTR:</td>
                    <td style="padding: 6px 0; font-weight: 700; text-align: right; color: #0d9488;">{{.CTR}}%</td>
                </tr>
            </table>
        </div>
        <p>You can view full real-time columnar telemetry logs in the ClickHouse-powered analytics panel.</p>
    </div>
</body>
</html>`))

// creativeModerationHTMLTemplate is the HTML body for creative moderation outcome updates.
var creativeModerationHTMLTemplate = template.Must(template.New("creative_moderation").Parse(`<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Operational Notification: Creative Moderation Update</title>
</head>
<body style="font-family: sans-serif; background-color: #f9fafb; color: #111827; margin: 0; padding: 40px 20px;">
    <div style="max-width: 576px; background-color: #ffffff; padding: 32px; border-radius: 12px; margin: auto; border: 1px solid #e5e7eb;">
        <div style="font-size: 20px; font-weight: 700; color: #1e3a8a; border-bottom: 2px solid #f3f4f6; padding-bottom: 16px; margin-bottom: 24px;">
            Ad Creative Moderation Update
        </div>
        <p>Hello,</p>
        <p>Your submitted creative has completed the verification phase with the following result:</p>
        <div style="background-color: #f8fafc; border: 1px solid #e2e8f0; padding: 16px; margin: 20px 0; border-radius: 8px;">
            Creative ID: <code>{{.CreativeID}}</code><br>
            Status: <strong>{{.Status}}</strong><br>
            {{if .Reason}}Reason: <span style="color: #b91c1c;">{{.Reason}}</span>{{end}}
        </div>
        <p>If approved, your creative will instantly begin bidding. If rejected, please revise the creative per the moderation reason provided.</p>
    </div>
</body>
</html>`))

// PasswordChangedEmailData carries template fields for password change security alerts.
type PasswordChangedEmailData struct {
	Email     string
	Time      string
	IP        string
	UserAgent string
}

// SendPasswordChangedEmail alerts the owner because a stolen session may have initiated the change.
func (m SlogMailer) SendPasswordChangedEmail(ctx context.Context, toEmail, clientIP, userAgent string) error {
	data := PasswordChangedEmailData{
		Email:     toEmail,
		Time:      time.Now().UTC().Format(time.RFC1123),
		IP:        clientIP,
		UserAgent: userAgent,
	}
	return renderAndLog(ctx, "password_changed", passwordChangedHTMLTemplate, toEmail, "Security Alert: Password Changed", data)
}

// NewIPLoginEmailData carries template fields for unfamiliar login location alerts.
type NewIPLoginEmailData struct {
	Email     string
	IP        string
	UserAgent string
	Time      string
}

// SendNewIPLoginEmail gives the owner a chance to react before further abuse from a new location.
func (m SlogMailer) SendNewIPLoginEmail(ctx context.Context, toEmail, clientIP, userAgent string) error {
	data := NewIPLoginEmailData{
		Email:     toEmail,
		IP:        clientIP,
		UserAgent: userAgent,
		Time:      time.Now().UTC().Format(time.RFC1123),
	}
	return renderAndLog(ctx, "new_ip_login", newIPLoginHTMLTemplate, toEmail, "Security Alert: Login from New IP", data)
}

// AccountLockedEmailData carries template fields for temporary account lockout notices.
type AccountLockedEmailData struct {
	Email        string
	IP           string
	LockDuration string
	Time         string
}

// SendAccountLockedEmail explains the lockout so users do not mistake it for an account deletion.
func (m SlogMailer) SendAccountLockedEmail(ctx context.Context, toEmail, clientIP, lockDuration string) error {
	data := AccountLockedEmailData{
		Email:        toEmail,
		IP:           clientIP,
		LockDuration: lockDuration,
		Time:         time.Now().UTC().Format(time.RFC1123),
	}
	return renderAndLog(ctx, "account_locked", accountLockedHTMLTemplate, toEmail, "Security Alert: Account Temporarily Locked", data)
}

// TwoFactorCodeEmailData carries template fields for two-factor verification codes.
type TwoFactorCodeEmailData struct {
	Email string
	Code  string
}

// Send2FACodeEmail delivers an out-of-band factor because password alone is insufficient for step-up.
func (m SlogMailer) Send2FACodeEmail(ctx context.Context, toEmail, code string) error {
	data := TwoFactorCodeEmailData{
		Email: toEmail,
		Code:  code,
	}
	return renderAndLog(ctx, "two_factor_code", twoFactorCodeHTMLTemplate, toEmail, "Your Two-Factor Verification Code", data)
}

// TopUpBalanceEmailData carries template fields for successful balance top-up confirmations.
type TopUpBalanceEmailData struct {
	Email    string
	Amount   string
	Currency string
}

// SendTopUpBalanceEmail confirms funds landed because billing disputes start from missing receipts.
func (m SlogMailer) SendTopUpBalanceEmail(ctx context.Context, toEmail, amount, currency string) error {
	data := TopUpBalanceEmailData{
		Email:    toEmail,
		Amount:   amount,
		Currency: currency,
	}
	return renderAndLog(ctx, "topup_balance", topUpBalanceHTMLTemplate, toEmail, "Billing: Balance Top-Up Successful", data)
}

// LowBalanceAlertEmailData carries template fields for low balance warnings before delivery stops.
type LowBalanceAlertEmailData struct {
	Email          string
	CurrentBalance string
	RemainingHours string
}

// SendLowBalanceAlertEmail warns before delivery stops for prepaid accounts with thin runway.
func (m SlogMailer) SendLowBalanceAlertEmail(ctx context.Context, toEmail, currentBalance, remainingHours string) error {
	data := LowBalanceAlertEmailData{
		Email:          toEmail,
		CurrentBalance: currentBalance,
		RemainingHours: remainingHours,
	}
	return renderAndLog(ctx, "low_balance_alert", lowBalanceAlertHTMLTemplate, toEmail, "Billing Alert: Low Balance Notice", data)
}

// MonthlyInvoiceEmailData carries template fields for monthly billing statement notices.
type MonthlyInvoiceEmailData struct {
	Email  string
	Period string
	Amount string
}

// SendMonthlyInvoiceEmail prompts review because spend reconciliation depends on timely statements.
func (m SlogMailer) SendMonthlyInvoiceEmail(ctx context.Context, toEmail, period, amount string) error {
	data := MonthlyInvoiceEmailData{
		Email:  toEmail,
		Period: period,
		Amount: amount,
	}
	return renderAndLog(ctx, "monthly_invoice", monthlyInvoiceHTMLTemplate, toEmail, "Billing: Monthly Statement Available", data)
}

// CampaignDepletedEmailData carries template fields for campaign budget exhaustion alerts.
type CampaignDepletedEmailData struct {
	Email        string
	CampaignName string
	CampaignID   string
}

// SendCampaignDepletedEmail explains why delivery halted so budgets are not misread as platform faults.
func (m SlogMailer) SendCampaignDepletedEmail(ctx context.Context, toEmail, campaignName, campaignID string) error {
	data := CampaignDepletedEmailData{
		Email:        toEmail,
		CampaignName: campaignName,
		CampaignID:   campaignID,
	}
	return renderAndLog(ctx, "campaign_depleted", campaignDepletedHTMLTemplate, toEmail, "Operational Notification: Campaign Budget Depleted", data)
}

// WeeklyPerformanceEmailData carries template fields for weekly campaign performance summaries.
type WeeklyPerformanceEmailData struct {
	Email       string
	Clicks      string
	Impressions string
	CTR         string
}

// SendWeeklyPerformanceEmail surfaces trends without requiring dashboard access for every stakeholder.
func (m SlogMailer) SendWeeklyPerformanceEmail(ctx context.Context, toEmail, clicks, impressions, ctr string) error {
	data := WeeklyPerformanceEmailData{
		Email:       toEmail,
		Clicks:      clicks,
		Impressions: impressions,
		CTR:         ctr,
	}
	return renderAndLog(ctx, "weekly_performance", weeklyPerformanceHTMLTemplate, toEmail, "Operational Statement: Weekly Analytics Performance Report", data)
}

// CreativeModerationEmailData carries template fields for creative moderation outcome updates.
type CreativeModerationEmailData struct {
	Email      string
	CreativeID string
	Status     string
	Reason     string
}

// SendCreativeModerationEmail closes the loop because rejected creatives otherwise look stuck in review.
func (m SlogMailer) SendCreativeModerationEmail(ctx context.Context, toEmail, creativeID, status, reason string) error {
	data := CreativeModerationEmailData{
		Email:      toEmail,
		CreativeID: creativeID,
		Status:     status,
		Reason:     reason,
	}
	return renderAndLog(ctx, "creative_moderation", creativeModerationHTMLTemplate, toEmail, "Operational Notification: Creative Moderation Update", data)
}
