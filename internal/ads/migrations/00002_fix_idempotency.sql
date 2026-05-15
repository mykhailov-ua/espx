-- +goose Up
-- +goose StatementBegin

-- Recreate events table with DATE-based partitioning.
-- PK (click_id, created_date) for deduplication.
-- created_date is a regular column (not GENERATED) because Postgres does not allow
-- generated columns in partition keys. The application layer sets it to created_at::date.
DROP TABLE IF EXISTS events_default;
DROP TABLE IF EXISTS events CASCADE;

CREATE TABLE events (
    click_id TEXT NOT NULL,
    campaign_id UUID NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('impression', 'click', 'conversion')),
    payload JSONB NOT NULL DEFAULT '{}',
    ip_address TEXT,
    user_agent TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_date DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (click_id, created_date)
) PARTITION BY RANGE (created_date);

CREATE TABLE events_default PARTITION OF events DEFAULT;

CREATE INDEX idx_events_campaign_id ON events(campaign_id);
CREATE INDEX idx_events_created_at ON events(created_at);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP TABLE IF EXISTS events_default;
DROP TABLE IF EXISTS events CASCADE;

CREATE TABLE events (
    click_id TEXT NOT NULL,
    campaign_id UUID NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('impression', 'click', 'conversion')),
    payload JSONB NOT NULL DEFAULT '{}',
    ip_address TEXT,
    user_agent TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (click_id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE events_default PARTITION OF events DEFAULT;

CREATE INDEX idx_events_campaign_id ON events(campaign_id);

-- +goose StatementEnd
