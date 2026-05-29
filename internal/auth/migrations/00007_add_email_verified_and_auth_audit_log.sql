-- +goose Up
-- +goose StatementBegin

-- Add email_verified, force explicit email ownership, default: FALSE, non-NULL.
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS email_verified BOOLEAN NOT NULL DEFAULT FALSE;

-- Fast filter for unverified emails.
CREATE INDEX IF NOT EXISTS idx_users_email_verified ON users(email_verified) WHERE email_verified = FALSE;

-- Auth service audit log, append-only, standalone (no user FK), immutable for compliance.
CREATE TABLE IF NOT EXISTS auth_audit_log (
    id BIGSERIAL PRIMARY KEY,
    user_id UUID,
    action TEXT NOT NULL,
    target_type TEXT,
    target_id TEXT,
    client_ip TEXT,
    user_agent TEXT,
    changes JSONB NOT NULL DEFAULT '{}',
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Last-N per user, for timeline/history.
CREATE INDEX IF NOT EXISTS idx_auth_audit_log_user_created
    ON auth_audit_log (user_id, created_at DESC);

-- For retention janitor TTL/cleanup.
CREATE INDEX IF NOT EXISTS idx_auth_audit_log_created_at
    ON auth_audit_log (created_at);

-- Dashboard/statistics on action x time.
CREATE INDEX IF NOT EXISTS idx_auth_audit_log_action_created
    ON auth_audit_log (action, created_at DESC);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_auth_audit_log_action_created;
DROP INDEX IF EXISTS idx_auth_audit_log_created_at;
DROP INDEX IF EXISTS idx_auth_audit_log_user_created;
DROP TABLE IF EXISTS auth_audit_log;

ALTER TABLE users DROP COLUMN IF EXISTS email_verified;
DROP INDEX IF EXISTS idx_users_email_verified;
-- +goose StatementEnd
