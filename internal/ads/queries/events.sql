-- events.sql: sqlc query definitions for campaign and event persistence.
-- All event writes target the PARTITION BY RANGE (created_date) table; callers
-- must supply created_date explicitly to guarantee correct partition routing.
-- ON CONFLICT (click_id, created_date) DO NOTHING provides idempotency within
-- the daily partition boundary.
--
-- InsertEventsBatch is the primary batch-write path. The CTE uses RETURNING to
-- count only newly-inserted rows for campaign_stats aggregation; this prevents
-- double-counting when the same batch is retried. The EXISTS sub-select on campaigns
-- filters orphaned campaign_ids before the stats INSERT to avoid FK violations
-- rolling back the entire batch.

-- name: CreateCampaign :one
INSERT INTO campaigns (id, name, budget_limit, status, customer_id, pacing_mode, daily_budget, timezone, freq_limit, freq_window, target_countries, brand_id, brand_fcap_key)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
RETURNING *;

-- name: GetCampaign :one
SELECT * FROM campaigns WHERE id = $1 LIMIT 1;

-- name: InsertEvent :exec
-- Inserts a single event with ON CONFLICT for idempotency.
-- created_date is set explicitly for correct dedup within daily partitions.
INSERT INTO events (click_id, campaign_id, user_id, event_type, payload, ip_address, user_agent, created_at, created_date)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (click_id, created_date) DO NOTHING;

-- name: UpdateCampaignStats :exec
INSERT INTO campaign_stats (campaign_id, date, impressions_count, clicks_count, conversions_count)
VALUES ($1, CURRENT_DATE, $2, $3, $4)
ON CONFLICT (campaign_id, date) DO UPDATE SET
    impressions_count = campaign_stats.impressions_count + EXCLUDED.impressions_count,
    clicks_count = campaign_stats.clicks_count + EXCLUDED.clicks_count,
    conversions_count = campaign_stats.conversions_count + EXCLUDED.conversions_count;

-- name: GetCampaignStats :many
SELECT * FROM campaign_stats 
WHERE campaign_id = $1 
ORDER BY date DESC;

-- name: ListCampaignIDs :many
SELECT id FROM campaigns WHERE status = 'ACTIVE';

-- name: UpdateCampaignStatsBatch :exec
INSERT INTO campaign_stats (campaign_id, date, impressions_count, clicks_count, conversions_count)
SELECT 
    val.campaign_id,
    CURRENT_DATE,
    val.impression,
    val.click,
    val.conversion
FROM (
    SELECT 
        unnest(@campaign_ids::uuid[]) as campaign_id,
        unnest(@impressions::bigint[]) as impression,
        unnest(@clicks::bigint[]) as click,
        unnest(@conversions::bigint[]) as conversion
) val
ORDER BY val.campaign_id
ON CONFLICT (campaign_id, date) DO UPDATE SET
    impressions_count = campaign_stats.impressions_count + EXCLUDED.impressions_count,
    clicks_count = campaign_stats.clicks_count + EXCLUDED.clicks_count,
    conversions_count = campaign_stats.conversions_count + EXCLUDED.conversions_count;

-- name: InsertEventsBatch :exec
-- Performs batch insert and atomically updates campaign stats.
-- Exactly-once aggregation is guaranteed because only newly inserted rows are counted.
-- Stats are attributed to the event's actual date (created_date), not CURRENT_DATE.
-- Invalid campaign_ids are filtered out before the stats insert to prevent FK violations
-- from rolling back the entire batch.
WITH inserted AS (
    INSERT INTO events (click_id, campaign_id, user_id, event_type, payload, ip_address, user_agent, created_at, created_date)
    SELECT 
        unnest(@click_ids::text[]),
        unnest(@campaign_ids::uuid[]),
        unnest(@user_ids::text[]),
        unnest(@event_types::text[]),
        unnest(@payloads::jsonb[]),
        unnest(@ip_addresses::text[]),
        unnest(@user_agents::text[]),
        unnest(@created_at::timestamptz[]),
        unnest(@created_dates::date[])
    ON CONFLICT (click_id, created_date) DO NOTHING
    RETURNING campaign_id, event_type, created_date
),
stats AS (
    SELECT i.campaign_id,
           i.created_date as event_date,
           COUNT(*) FILTER (WHERE i.event_type = 'impression') as imps,
           COUNT(*) FILTER (WHERE i.event_type = 'click') as clicks,
           COUNT(*) FILTER (WHERE i.event_type = 'conversion') as convs
    FROM inserted i
    WHERE EXISTS (SELECT 1 FROM campaigns c WHERE c.id = i.campaign_id)
    GROUP BY i.campaign_id, i.created_date
    ORDER BY i.campaign_id, i.created_date
)
INSERT INTO campaign_stats (campaign_id, date, impressions_count, clicks_count, conversions_count)
SELECT campaign_id, event_date, imps, clicks, convs
FROM stats
ORDER BY campaign_id, event_date
ON CONFLICT (campaign_id, date) DO UPDATE SET
    impressions_count = campaign_stats.impressions_count + EXCLUDED.impressions_count,
    clicks_count = campaign_stats.clicks_count + EXCLUDED.clicks_count,
    conversions_count = campaign_stats.conversions_count + EXCLUDED.conversions_count;
