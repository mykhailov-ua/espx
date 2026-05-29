-- +goose Up
-- +goose StatementBegin
-- password_history: enforce SOC2/PCI-DSS password reuse ban, Argon2id hashes only. Append-only, per-user lookup by (user_id, created_at DESC).
CREATE TABLE IF NOT EXISTS password_history (
    id BIGSERIAL PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_password_history_user_created ON password_history (user_id, created_at DESC);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_password_history_user_created;
DROP TABLE IF EXISTS password_history;
-- +goose StatementEnd
