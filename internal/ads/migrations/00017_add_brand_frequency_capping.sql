-- +goose Up
-- +goose StatementBegin

CREATE TABLE advertiser_brands (
    id UUID PRIMARY KEY,
    customer_id UUID NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_advertiser_brands_customer_id ON advertiser_brands(customer_id);

ALTER TABLE campaigns 
ADD COLUMN brand_id UUID REFERENCES advertiser_brands(id) ON DELETE SET NULL,
ADD COLUMN brand_fcap_key TEXT NOT NULL DEFAULT '';

UPDATE campaigns SET brand_fcap_key = 'fcap:c:' || id::text;

CREATE INDEX idx_campaigns_brand_id ON campaigns(brand_id);
CREATE INDEX idx_campaigns_brand_fcap_key ON campaigns(brand_fcap_key);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP INDEX IF EXISTS idx_campaigns_brand_fcap_key;
DROP INDEX IF EXISTS idx_campaigns_brand_id;

ALTER TABLE campaigns 
DROP COLUMN IF EXISTS brand_fcap_key,
DROP COLUMN IF EXISTS brand_id;

DROP TABLE IF EXISTS advertiser_brands;

-- +goose StatementEnd
