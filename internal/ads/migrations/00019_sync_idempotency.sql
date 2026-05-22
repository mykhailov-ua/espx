-- +goose Up
-- +goose StatementBegin
CREATE TABLE sync_idempotency (
    id TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE sync_idempotency;
-- +goose StatementEnd
