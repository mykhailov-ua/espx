-- +goose Up
ALTER TABLE campaigns 
ADD COLUMN target_countries TEXT[];

COMMENT ON COLUMN campaigns.target_countries IS 'Array of allowed ISO country codes. NULL or empty means all countries allowed.';

CREATE INDEX idx_campaigns_geo ON campaigns USING GIN (target_countries) WHERE status = 'ACTIVE';

-- +goose Down
DROP INDEX IF EXISTS idx_campaigns_geo;
ALTER TABLE campaigns DROP COLUMN target_countries;
