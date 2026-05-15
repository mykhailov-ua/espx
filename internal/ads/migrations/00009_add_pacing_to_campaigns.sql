-- +goose Up
-- +goose StatementBegin
CREATE TYPE pacing_mode_type AS ENUM ('ASAP', 'EVEN');

ALTER TABLE campaigns 
ADD COLUMN pacing_mode pacing_mode_type NOT NULL DEFAULT 'ASAP',
ADD COLUMN daily_budget DECIMAL(15,2) NOT NULL DEFAULT 0,
ADD COLUMN timezone TEXT NOT NULL DEFAULT 'UTC';

-- Create index for lookup by status and pacing for background jobs if needed
CREATE INDEX idx_campaigns_pacing ON campaigns(pacing_mode) WHERE status = 'ACTIVE';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE campaigns DROP COLUMN IF EXISTS pacing_mode;
ALTER TABLE campaigns DROP COLUMN IF EXISTS daily_budget;
ALTER TABLE campaigns DROP COLUMN IF EXISTS timezone;
DROP TYPE IF EXISTS pacing_mode_type;
-- +goose StatementEnd
