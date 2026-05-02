-- name: CreateCampaign :one
INSERT INTO campaigns (id, name, budget, status)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: GetCampaign :one
SELECT * FROM campaigns WHERE id = $1 LIMIT 1;

-- name: InsertEvent :exec
-- Inserts a single event with ON CONFLICT for idempotency.
INSERT INTO events (click_id, campaign_id, event_type, payload, ip_address, user_agent, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (click_id, created_at) DO NOTHING;

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
SELECT id FROM campaigns WHERE status = 'active';

-- name: UpdateCampaignStatsBatch :exec
INSERT INTO campaign_stats (campaign_id, date, impressions_count, clicks_count, conversions_count)
SELECT 
    unnest(@campaign_ids::uuid[]),
    CURRENT_DATE,
    unnest(@impressions::bigint[]),
    unnest(@clicks::bigint[]),
    unnest(@conversions::bigint[])
ON CONFLICT (campaign_id, date) DO UPDATE SET
    impressions_count = campaign_stats.impressions_count + EXCLUDED.impressions_count,
    clicks_count = campaign_stats.clicks_count + EXCLUDED.clicks_count,
    conversions_count = campaign_stats.conversions_count + EXCLUDED.conversions_count;

-- name: InsertEventsBatch :exec
-- Performs batch insert and atomically updates campaign stats.
-- Exactly-once aggregation is guaranteed because only newly inserted rows are counted.
WITH inserted AS (
    INSERT INTO events (click_id, campaign_id, event_type, payload, ip_address, user_agent, created_at)
    SELECT 
        unnest(@click_ids::text[]),
        unnest(@campaign_ids::uuid[]),
        unnest(@event_types::text[]),
        unnest(@payloads::jsonb[]),
        unnest(@ip_addresses::text[]),
        unnest(@user_agents::text[]),
        unnest(@created_at::timestamptz[])
    ON CONFLICT (click_id, created_at) DO NOTHING
    RETURNING campaign_id, event_type
),
stats AS (
    SELECT campaign_id,
           COUNT(*) FILTER (WHERE event_type = 'impression') as imps,
           COUNT(*) FILTER (WHERE event_type = 'click') as clicks,
           COUNT(*) FILTER (WHERE event_type = 'conversion') as convs
    FROM inserted
    GROUP BY campaign_id
)
INSERT INTO campaign_stats (campaign_id, date, impressions_count, clicks_count, conversions_count)
SELECT campaign_id, CURRENT_DATE, imps, clicks, convs
FROM stats
ON CONFLICT (campaign_id, date) DO UPDATE SET
    impressions_count = campaign_stats.impressions_count + EXCLUDED.impressions_count,
    clicks_count = campaign_stats.clicks_count + EXCLUDED.clicks_count,
    conversions_count = campaign_stats.conversions_count + EXCLUDED.conversions_count;
