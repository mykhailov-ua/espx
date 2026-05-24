-- name: CreateCustomer :one
INSERT INTO customers (id, name, balance, currency)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: UpdateCustomerBalanceManagement :one
UPDATE customers
SET balance = balance + $2,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
RETURNING *;

-- name: GetCustomerForUpdate :one
SELECT * FROM customers
WHERE id = $1
FOR UPDATE;

-- name: UpdateCampaignStatus :one
UPDATE campaigns
SET status = $2,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
RETURNING *;

-- name: GetCampaignFull :one
SELECT * FROM campaigns
WHERE id = $1;

-- name: CreateLedgerEntry :one
INSERT INTO balance_ledger (customer_id, campaign_id, amount, type, idempotency_hash)
VALUES ($1, $2, $3, $4, $5)
RETURNING *;

-- name: GetLedgerByHash :one
SELECT * FROM balance_ledger
WHERE idempotency_hash = $1;

-- name: GetLedgerByHashForUpdate :one
SELECT * FROM balance_ledger
WHERE idempotency_hash = $1
FOR UPDATE;

-- name: CreateStatusHistory :exec
INSERT INTO campaign_status_history (campaign_id, old_status, new_status, reason)
VALUES ($1, $2, $3, $4);

-- name: SoftDeleteCampaign :exec
UPDATE campaigns
SET status = 'DELETED',
    deleted_at = CURRENT_TIMESTAMP
WHERE id = $1;

-- name: CreateAuditLog :one
INSERT INTO admin_audit_log (admin_id, action, target_type, target_id, changes, metadata)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING *;

-- name: CleanupAuditLogs :exec
DELETE FROM admin_audit_log
WHERE created_at < $1;

-- name: ListAuditLogs :many
SELECT * FROM admin_audit_log
ORDER BY created_at DESC
LIMIT $1 OFFSET $2;

-- name: CountCustomers :one
SELECT COUNT(*) FROM customers;

-- name: ListCustomers :many
SELECT * FROM customers
ORDER BY created_at DESC
LIMIT $1 OFFSET $2;


-- name: GetCustomerStats :many
SELECT customer_id, COUNT(*) as active_campaigns, COALESCE(SUM(current_spend), 0)::bigint as total_spend
FROM campaigns
WHERE customer_id = ANY(@customer_ids::uuid[]) AND status = 'ACTIVE'
GROUP BY customer_id;

-- name: CountCustomerLedger :one
SELECT COUNT(*) FROM balance_ledger
WHERE customer_id = $1;

-- name: ListCustomerLedger :many
SELECT * FROM balance_ledger
WHERE customer_id = $1
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;

-- name: CountCampaigns :one
SELECT COUNT(*) FROM campaigns
WHERE (sqlc.narg('customer_id')::uuid IS NULL OR customer_id = sqlc.narg('customer_id')::uuid)
  AND (sqlc.narg('status')::text IS NULL OR status::text = sqlc.narg('status')::text);

-- name: ListCampaigns :many
SELECT * FROM campaigns
WHERE (sqlc.narg('customer_id')::uuid IS NULL OR customer_id = sqlc.narg('customer_id')::uuid)
  AND (sqlc.narg('status')::text IS NULL OR status::text = sqlc.narg('status')::text)
ORDER BY created_at DESC
LIMIT $1 OFFSET $2;

-- name: CountStatusHistory :one
SELECT COUNT(*) FROM campaign_status_history
WHERE campaign_id = $1;

-- name: ListStatusHistory :many
SELECT * FROM campaign_status_history
WHERE campaign_id = $1
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;

-- name: CreateBlacklistIP :one
INSERT INTO ip_blacklist (ip, reason)
VALUES ($1, $2)
ON CONFLICT (ip) DO UPDATE SET reason = EXCLUDED.reason, created_at = CURRENT_TIMESTAMP
RETURNING *;

-- name: DeleteBlacklistIP :exec
DELETE FROM ip_blacklist
WHERE ip = $1;

-- name: CountBlacklist :one
SELECT COUNT(*) FROM ip_blacklist;

-- name: ListBlacklist :many
SELECT * FROM ip_blacklist
ORDER BY created_at DESC
LIMIT $1 OFFSET $2;

-- name: GetAllBlacklist :many
SELECT ip, reason FROM ip_blacklist;

-- name: SetSystemSetting :exec
INSERT INTO system_settings (key, value, updated_at)
VALUES ($1, $2, CURRENT_TIMESTAMP)
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP;

-- name: GetAllSystemSettings :many
SELECT key, value FROM system_settings;

-- name: CreateOutboxEvent :one
INSERT INTO outbox_events (event_type, payload)
VALUES ($1, $2)
RETURNING *;

-- name: GetPendingOutboxEventsForUpdate :many
SELECT * FROM outbox_events
WHERE status = 'PENDING'
ORDER BY created_at ASC
LIMIT $1
FOR UPDATE SKIP LOCKED;

-- name: MarkOutboxEventProcessed :exec
UPDATE outbox_events
SET status = 'PROCESSED'
WHERE id = $1;

-- name: GetDrainingCampaignsForUpdate :many
SELECT * FROM campaigns
WHERE status = 'DRAINING' AND updated_at < $1
ORDER BY updated_at ASC
LIMIT $2
FOR UPDATE SKIP LOCKED;

-- name: ListCustomersForScoring :many
SELECT 
    c.id,
    COALESCE(FLOOR(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - c.created_at)) / 86400), 0)::integer AS age_days,
    COALESCE(SUM(l.amount), 0)::bigint AS topup_sum_30d
FROM customers c
LEFT JOIN balance_ledger l ON l.customer_id = c.id 
    AND l.type = 'TOPUP' 
    AND l.created_at >= CURRENT_TIMESTAMP - INTERVAL '30 days'
GROUP BY c.id;

-- name: UpdateCustomerOverdraft :one
UPDATE customers
SET allowed_overdraft = $2,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
RETURNING *;

-- name: CreateBrand :one
INSERT INTO advertiser_brands (id, customer_id, name)
VALUES ($1, $2, $3)
RETURNING *;

-- name: GetBrand :one
SELECT * FROM advertiser_brands WHERE id = $1 LIMIT 1;

-- name: GetBrandForUpdate :one
SELECT * FROM advertiser_brands WHERE id = $1 LIMIT 1 FOR UPDATE;

-- name: ConfigureBrandFcap :exec
UPDATE advertiser_brands
SET freq_limit = $2,
    freq_window = $3,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1;

-- name: ListBrandsByCustomer :many
SELECT * FROM advertiser_brands
WHERE customer_id = $1
ORDER BY created_at DESC;

-- name: GetCampaignsWithStats :many
SELECT 
    c.id, c.name, c.status, c.budget_limit, c.created_at, c.updated_at, c.customer_id, c.current_spend, c.deleted_at, c.pacing_mode, c.daily_budget, c.timezone, c.freq_limit, c.freq_window, c.target_countries, c.brand_id, c.brand_fcap_key,
    COALESCE(SUM(s.impressions_count), 0)::bigint AS total_impressions,
    COALESCE(SUM(s.clicks_count), 0)::bigint AS total_clicks,
    COALESCE(SUM(s.conversions_count), 0)::bigint AS total_conversions
FROM campaigns c
LEFT JOIN campaign_stats s ON c.id = s.campaign_id
WHERE c.customer_id = $1 AND c.status = 'ACTIVE'
GROUP BY c.id;

-- name: UpdateCampaignBudget :one
UPDATE campaigns
SET budget_limit = $2,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
RETURNING *;

-- name: GetAllActiveCampaignsWithStats :many
SELECT 
    c.id, c.name, c.status, c.budget_limit, c.created_at, c.updated_at, c.customer_id, c.current_spend, c.deleted_at, c.pacing_mode, c.daily_budget, c.timezone, c.freq_limit, c.freq_window, c.target_countries, c.brand_id, c.brand_fcap_key,
    COALESCE(SUM(s.impressions_count), 0)::bigint AS total_impressions,
    COALESCE(SUM(s.clicks_count), 0)::bigint AS total_clicks,
    COALESCE(SUM(s.conversions_count), 0)::bigint AS total_conversions
FROM campaigns c
LEFT JOIN campaign_stats s ON c.id = s.campaign_id
WHERE c.status = 'ACTIVE'
GROUP BY c.id;

-- name: GetCampaignForUpdate :one
SELECT * FROM campaigns
WHERE id = $1
FOR UPDATE;

-- name: UpdateCampaignPacing :one
UPDATE campaigns
SET pacing_mode = $2,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
RETURNING *;
