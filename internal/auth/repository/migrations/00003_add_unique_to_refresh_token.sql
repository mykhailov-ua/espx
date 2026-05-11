-- +goose Up
-- +goose StatementBegin
ALTER TABLE sessions ADD CONSTRAINT sessions_refresh_token_unique UNIQUE (refresh_token);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE sessions DROP CONSTRAINT IF EXISTS sessions_refresh_token_unique;
-- +goose StatementEnd
