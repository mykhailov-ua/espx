#!/bin/bash
set -e

echo "Stopping and cleaning up containers (including orphans)"
docker compose down --remove-orphans

echo "Starting services"
docker compose up -d --build --remove-orphans

echo "Waiting for services to become healthy"
echo "Waiting for Postgres..."
until docker exec espx-db-1 pg_isready -p 5440 -U ad_event_processor_user -d ad_event_processor >/dev/null 2>&1; do
  sleep 1
done

echo "Waiting for ClickHouse..."
until docker exec espx-clickhouse-1 wget -qO- http://127.0.0.1:8123/ping >/dev/null 2>&1; do
  sleep 1
done

echo "Waiting for Redis shards..."
for i in 0 1 2 3 4 5; do
  until docker exec espx-redis-$i-1 redis-cli -p 6379 -a redis_secure_pass_456 ping >/dev/null 2>&1; do
    sleep 1
  done
done

echo "Cleaning Redis shards"
for i in 0 1 2 3 4 5; do
  docker exec espx-redis-$i-1 redis-cli -p 6379 -a redis_secure_pass_456 FLUSHALL >/dev/null 2>&1
done

echo "Resetting Postgres database"
docker exec -i espx-db-1 psql -h localhost -p 5440 -U ad_event_processor_user -d ad_event_processor <<'EOF'
TRUNCATE TABLE events CASCADE;
TRUNCATE TABLE campaign_stats CASCADE;

-- Insert 100 customers with huge balance
INSERT INTO customers (id, name, balance, currency, allowed_overdraft)
SELECT 
    ('00000000-0000-0000-0000-' || LPAD(to_hex(i), 12, '0'))::uuid,
    'Test Customer ' || i,
    100000000000000,
    'USD',
    0
FROM generate_series(1, 100) s(i)
ON CONFLICT (id) DO UPDATE SET balance = 100000000000000;

-- Insert 100 active campaigns with huge budget
INSERT INTO campaigns (id, name, budget_limit, status, customer_id, pacing_mode, daily_budget, timezone, freq_limit, freq_window)
SELECT 
    ('00000000-0000-0000-0000-' || LPAD(to_hex(i), 12, '0'))::uuid,
    'Test Campaign ' || i,
    100000000000000,
    'ACTIVE',
    ('00000000-0000-0000-0000-' || LPAD(to_hex(i), 12, '0'))::uuid,
    'ASAP',
    100000000000000,
    'UTC',
    100000000,
    3600
FROM generate_series(1, 100) s(i)
ON CONFLICT (id) DO UPDATE SET current_spend = 0, status = 'ACTIVE', budget_limit = 100000000000000, daily_budget = 100000000000000, freq_limit = 100000000;
EOF

echo "Resetting ClickHouse database"
docker exec -i espx-clickhouse-1 clickhouse-client --multiquery -u default --password secure_ch_pass -d ad_event_processor -q "
TRUNCATE TABLE impressions;
TRUNCATE TABLE clicks;
TRUNCATE TABLE conversions;
TRUNCATE TABLE fraud_events;
"

echo "Restarting trackers and processor to recreate consumer groups"
docker compose restart processor tracker-0 tracker-1 tracker-2 tracker-3

echo "Triggering campaign registry sync via Redis Pub/Sub"
for i in $(seq 1 100); do
  hex=$(printf "%012x" $i)
  uuid="00000000-0000-0000-0000-$hex"
  docker exec espx-redis-0-1 redis-cli -p 6379 -a redis_secure_pass_456 PUBLISH campaigns:update "$uuid" >/dev/null 2>&1
done

echo "Verification"
echo "Active campaign count in Postgres:"
docker exec espx-db-1 psql -h localhost -p 5440 -U ad_event_processor_user -d ad_event_processor -c "SELECT COUNT(*) FROM campaigns WHERE status = 'ACTIVE';"

echo "All systems ready for load test!"
