\set QUIET 1

DROP TABLE IF EXISTS tmp_auth_audit_seed;
CREATE TEMP TABLE tmp_auth_audit_seed (LIKE auth_audit_log INCLUDING ALL);

INSERT INTO tmp_auth_audit_seed (user_id, action, target_type, target_id, client_ip, user_agent, changes, metadata, created_at)
SELECT
    ('00000000-0000-0000-0000-' || lpad((gs % 200)::text, 12, '0'))::uuid,
    (ARRAY['LOGIN_SUCCESS','LOGIN_FAILURE','PASSWORD_CHANGED','API_KEY_CREATED','EMAIL_VERIFIED','TOKEN_REFRESH','ACCOUNT_LOCKED'])[1 + (random()*6)::int],
    'user',
    ('00000000-0000-0000-0000-' || lpad((gs % 200)::text, 12, '0')),
    ('10.0.' || (gs % 255) || '.' || ((gs/255) % 255)),
    (ARRAY['Mozilla/5.0 (Macintosh)','Mozilla/5.0 (Windows)','curl/8.0','Go-http-client/1.1'])[1+(random()*3)::int],
    jsonb_build_object('outcome', (ARRAY['ok','fail'])[1+(random()*1)::int]),
    jsonb_build_object('req_id', gen_random_uuid()),
    now() - ((random()*90)::int || ' days')::interval - ((random()*86400)::int || ' seconds')::interval
FROM generate_series(1, 10000) gs;

CREATE INDEX ON tmp_auth_audit_seed (user_id, created_at DESC);
CREATE INDEX ON tmp_auth_audit_seed (created_at);
CREATE INDEX ON tmp_auth_audit_seed (action, created_at DESC);

ANALYZE tmp_auth_audit_seed;

\echo
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM tmp_auth_audit_seed
WHERE user_id = '00000000-0000-0000-0000-000000000042'
ORDER BY created_at DESC
LIMIT 50 OFFSET 0;

\echo
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT count(*) FROM tmp_auth_audit_seed
WHERE action = 'PASSWORD_CHANGED'
  AND created_at >= now() - interval '7 days';

\echo
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
DELETE FROM tmp_auth_audit_seed
WHERE created_at < now() - interval '400 days';

\echo
DROP TABLE tmp_auth_audit_seed;
