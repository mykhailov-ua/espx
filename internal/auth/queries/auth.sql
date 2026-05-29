-- name: GetUserByEmail :one
SELECT id, email, password_hash, role, customer_id, created_at, updated_at, is_blocked, email_verified
FROM users
WHERE email = $1;

-- name: GetUserByID :one
SELECT id, email, password_hash, role, customer_id, created_at, updated_at, is_blocked, email_verified
FROM users
WHERE id = $1;

-- name: CreateUser :one
INSERT INTO users (email, password_hash, role, customer_id)
VALUES ($1, $2, $3, $4)
RETURNING id, email, role, customer_id, created_at;

-- name: UpdatePassword :exec
UPDATE users
SET password_hash = $2, updated_at = NOW()
WHERE email = $1;

-- name: BlockUser :exec
UPDATE users
SET is_blocked = TRUE, updated_at = NOW()
WHERE email = $1;

-- name: GetAPIKeyByHash :one
SELECT ak.id, ak.user_id, ak.name, ak.expires_at, u.role, u.customer_id
FROM api_keys ak
JOIN users u ON ak.user_id = u.id
WHERE ak.key_hash = $1 AND (ak.expires_at IS NULL OR ak.expires_at > NOW());

-- name: CreateAPIKey :one
INSERT INTO api_keys (key_hash, user_id, name, expires_at)
VALUES ($1, $2, $3, $4)
RETURNING id, name, expires_at, created_at;

-- name: ListUserAPIKeys :many
SELECT id, name, expires_at, created_at
FROM api_keys
WHERE user_id = $1;

-- name: CreateSession :one
INSERT INTO sessions (id, user_id, refresh_token, user_agent, client_ip, is_blocked, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING id, user_id, refresh_token, user_agent, client_ip, is_blocked, expires_at, created_at;

-- name: GetSession :one
SELECT id, user_id, refresh_token, user_agent, client_ip, is_blocked, expires_at, created_at
FROM sessions
WHERE id = $1;

-- name: GetSessionByRefreshToken :one
SELECT id, user_id, refresh_token, user_agent, client_ip, is_blocked, expires_at, created_at
FROM sessions
WHERE refresh_token = $1;

-- name: GetSessionByRefreshTokenForUpdate :one
SELECT id, user_id, refresh_token, user_agent, client_ip, is_blocked, expires_at, created_at
FROM sessions
WHERE refresh_token = $1
FOR UPDATE;

-- name: BlockSession :exec
UPDATE sessions
SET is_blocked = TRUE
WHERE id = $1;

-- name: BlockSessionByRefreshToken :exec
UPDATE sessions
SET is_blocked = TRUE
WHERE refresh_token = $1;

-- name: DeleteExpiredOrBlockedSessions :execrows
DELETE FROM sessions
WHERE expires_at < NOW() OR is_blocked = TRUE;

-- name: SetEmailVerified :exec
UPDATE users
SET email_verified = TRUE, updated_at = NOW()
WHERE id = $1;

-- name: CreateAuthAuditLog :one
INSERT INTO auth_audit_log (user_id, action, target_type, target_id, client_ip, user_agent, changes, metadata)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING id, created_at;

-- name: ListAuthAuditLogsByUser :many
SELECT id, user_id, action, target_type, target_id, client_ip, user_agent, changes, metadata, created_at
FROM auth_audit_log
WHERE user_id = $1
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;

-- name: CreatePasswordHistoryEntry :exec
INSERT INTO password_history (user_id, password_hash)
VALUES ($1, $2);

-- name: GetPasswordHistory :many
SELECT password_hash
FROM password_history
WHERE user_id = $1
ORDER BY created_at DESC
LIMIT $2;
