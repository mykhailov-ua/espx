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
TTL created_at + INTERVAL 180 DAY;

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
TTL created_at + INTERVAL 180 DAY;

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
TTL created_at + INTERVAL 180 DAY;

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
TTL created_at + INTERVAL 90 DAY;
