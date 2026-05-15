-- +goose Up
ALTER TABLE campaigns 
ADD COLUMN freq_limit INTEGER DEFAULT 0,
ADD COLUMN freq_window INTEGER DEFAULT 86400;

ALTER TABLE events ADD COLUMN user_id TEXT;
CREATE INDEX idx_events_user_id ON events(user_id);

COMMENT ON COLUMN campaigns.freq_limit IS 'Max events per user. 0 means unlimited.';
COMMENT ON COLUMN campaigns.freq_window IS 'Time window for frequency capping in seconds.';

-- +goose Down
DROP INDEX IF EXISTS idx_events_user_id;
ALTER TABLE events DROP COLUMN user_id;
ALTER TABLE campaigns 
DROP COLUMN freq_limit,
DROP COLUMN freq_window;
