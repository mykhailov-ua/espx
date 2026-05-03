-- +goose Up
-- +goose StatementBegin

-- Create Enum for Campaign Status
CREATE TYPE campaign_status_type AS ENUM ('ACTIVE', 'PAUSED', 'EXHAUSTED');

-- Create Customers table
CREATE TABLE customers (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    balance DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    currency TEXT NOT NULL DEFAULT 'USD',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Update Campaigns table
-- 1. Add customer_id link
ALTER TABLE campaigns ADD COLUMN customer_id UUID REFERENCES customers(id) ON DELETE CASCADE;

-- 2. Add current_spend
ALTER TABLE campaigns ADD COLUMN current_spend DECIMAL(15,2) NOT NULL DEFAULT 0.00;

-- 3. Rename budget to budget_limit for clarity
ALTER TABLE campaigns RENAME COLUMN budget TO budget_limit;

-- 4. Convert status column to Enum
DROP INDEX IF EXISTS idx_campaigns_status_active;
DROP INDEX IF EXISTS idx_campaigns_status;

ALTER TABLE campaigns ALTER COLUMN status DROP DEFAULT;
ALTER TABLE campaigns ALTER COLUMN status TYPE campaign_status_type 
    USING (CASE 
        WHEN LOWER(status) = 'active' THEN 'ACTIVE'::campaign_status_type 
        ELSE 'PAUSED'::campaign_status_type 
    END);

ALTER TABLE campaigns ALTER COLUMN status SET DEFAULT 'ACTIVE';

-- Indices for performance
CREATE INDEX idx_campaigns_customer_id ON campaigns(customer_id);
CREATE INDEX idx_campaigns_status_active ON campaigns(status) WHERE status = 'ACTIVE'::campaign_status_type;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE campaigns DROP COLUMN IF EXISTS current_spend;
ALTER TABLE campaigns DROP COLUMN IF EXISTS customer_id;
ALTER TABLE campaigns RENAME COLUMN budget_limit TO budget;
ALTER TABLE campaigns ALTER COLUMN status TYPE TEXT USING (status::TEXT);
DROP TABLE IF EXISTS customers;
DROP TYPE IF EXISTS campaign_status_type;
-- +goose StatementEnd
