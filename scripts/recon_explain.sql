\set QUIET 1

-- Query simulates closed reconciliation window (real workloads: many campaigns, hour-scale row span)
INSERT INTO recon_runs (period_start, period_end, status, total_delta, campaigns_checked, discrepancies_found)
VALUES (NOW() - INTERVAL '3 hours', NOW() - INTERVAL '2 hours', 'COMPLETED', 123456789, 4821, 17)
ON CONFLICT DO NOTHING;

-- Core hot path - ReconcileWindow windowed aggregate (see index expectations below)
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT 
    campaign_id,
    COALESCE(SUM(CASE WHEN amount < 0 THEN -amount ELSE 0 END), 0)::bigint AS total_spent_micro
FROM balance_ledger
WHERE created_at >= NOW() - INTERVAL '3 hours'
  AND created_at < NOW() - INTERVAL '2 hours'
  AND (type IN ('FEE', 'RECONCILIATION_ADJUST', 'REFUND'))
GROUP BY campaign_id
LIMIT 1000;

-- On large ledgers: efficient range scan requires composite/partial index on (created_at, type)
-- High-cardinality GROUP BY (UUID); BRIN or monthly partitioning recommended on created_at for very large tables
