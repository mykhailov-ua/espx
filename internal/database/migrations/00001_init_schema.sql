-- +goose Up
-- +goose StatementBegin
CREATE TABLE campaigns (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    budget DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE events (
    id UUID NOT NULL,
    campaign_id UUID NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('impression', 'click', 'conversion')),
    payload JSONB NOT NULL DEFAULT '{}',
    ip_address TEXT,
    user_agent TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE campaign_stats (
    campaign_id UUID NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    date DATE NOT NULL DEFAULT CURRENT_DATE,
    impressions_count BIGINT NOT NULL DEFAULT 0,
    clicks_count BIGINT NOT NULL DEFAULT 0,
    conversions_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (campaign_id, date)
);

-- Performance indexes
CREATE INDEX idx_events_campaign_id ON events(campaign_id);
CREATE INDEX idx_campaigns_status_active ON campaigns(status) WHERE status = 'active';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS campaign_stats;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS campaigns;
-- +goose StatementEnd
