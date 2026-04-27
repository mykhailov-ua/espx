-- name: CreateCampaign :one
INSERT INTO campaigns (id, name, budget, status)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: GetCampaign :one
SELECT * FROM campaigns WHERE id = $1 LIMIT 1;

-- name: InsertEvent :one
INSERT INTO events (campaign_id, event_type, payload, ip_address, user_agent)
VALUES ($1, $2, $3, $4, $5)
RETURNING *;

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
