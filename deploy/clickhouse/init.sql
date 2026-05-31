-- deploy/clickhouse/init.sql: ClickHouse schema initialisation for the ad-event pipeline.
-- All tables use ReplacingMergeTree(created_at) to deduplicate re-ingested events
-- based on the ORDER BY key; deduplication is eventually consistent and is triggered
-- by OPTIMIZE TABLE or background merges.
--
-- PARTITION BY toYYYYMM(created_at): monthly partitions enable efficient bulk-drop
-- for TTL enforcement and fast range scans in the reconciliation queries.
-- ORDER BY (campaign_id, created_at, click_id): optimises aggregation queries grouped
-- by campaign (reconciliation, reporting) while keeping click_id as a trailing sort
-- key for deduplication within the same campaign+timestamp bucket.
--
-- TTL: impressions/clicks/conversions are retained for 180 days; fraud_events for 90 days.
-- The TTL column must match or extend the replication lag window to avoid premature deletion.

CREATE DATABASE IF NOT EXISTS ad_event_processor;
USE ad_event_processor;

-- Impressions Table
CREATE TABLE IF NOT EXISTS impressions (
    click_id String,
    campaign_id UUID,
    ip_address String,
    user_agent String,
    payload String,
    created_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (campaign_id, created_at, click_id)
TTL toDateTime(created_at) + INTERVAL 180 DAY;

-- Clicks Table
CREATE TABLE IF NOT EXISTS clicks (
    click_id String,
    campaign_id UUID,
    ip_address String,
    user_agent String,
    payload String,
    created_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (campaign_id, created_at, click_id)
TTL toDateTime(created_at) + INTERVAL 180 DAY;

-- Conversions Table
CREATE TABLE IF NOT EXISTS conversions (
    click_id String,
    campaign_id UUID,
    ip_address String,
    user_agent String,
    payload String,
    created_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (campaign_id, created_at, click_id)
TTL toDateTime(created_at) + INTERVAL 180 DAY;

-- Fraud Events Table
CREATE TABLE IF NOT EXISTS fraud_events (
    click_id String,
    campaign_id UUID,
    user_id String,
    event_type String,
    ip_address String,
    user_agent String,
    payload String,
    fraud_reason String,
    created_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (campaign_id, created_at, click_id)
TTL toDateTime(created_at) + INTERVAL 90 DAY;
