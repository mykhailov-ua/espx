-- +goose Up
-- +goose StatementBegin
ALTER TABLE advertiser_brands
ADD COLUMN freq_limit INTEGER NOT NULL DEFAULT 0,
ADD COLUMN freq_window INTEGER NOT NULL DEFAULT 0;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE advertiser_brands
DROP COLUMN IF EXISTS freq_window,
DROP COLUMN IF EXISTS freq_limit;
-- +goose StatementEnd
