#!/usr/bin/env bash

set -eo pipefail

if [ -f .env ]; then
    echo "Loading environment variables from .env..."
    export $(grep -v '^#' .env | xargs)
fi

DB_PORT=${DB_PORT:-5440}
DB_USER=${DB_USER:-ad_event_processor_user}
DB_NAME=${DB_NAME:-ad_event_processor}
REDIS_PASSWORD=${REDIS_PASSWORD:-redis_secure_pass_456}
CH_HTTP_PORT=${CH_HTTP_PORT:-8123}

REDIS_PORTS=(6479 6480 6481 6482 6483 6484)

echo "Checking Local Environment Dependencies..."

check_port() {
    local host=$1
    local port=$2
    local name=$3
    if nc -z "$host" "$port" >/dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

echo -n "1. Checking PostgreSQL (port $DB_PORT)... "
if pg_isready -h 127.0.0.1 -p "$DB_PORT" -U "$DB_USER" >/dev/null 2>&1; then
    echo "HEALTHY (pg_isready)"
elif check_port 127.0.0.1 "$DB_PORT" "PostgreSQL"; then
    echo "HEALTHY (TCP port responsive)"
else
    echo "FAILED"
    echo "ERROR: PostgreSQL is not running or not accessible on 127.0.0.1:$DB_PORT"
    exit 1
fi

echo -n "   Checking PostgreSQL migrations... "
if which PGPASSWORD="$DB_PASSWORD" psql >/dev/null 2>&1; then
    if PGPASSWORD="$DB_PASSWORD" psql -h 127.0.0.1 -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "SELECT 1 FROM users, campaigns LIMIT 1;" >/dev/null 2>&1; then
        echo "MIGRATED (verified via tables check)"
    else
        echo "PENDING / EMPTY"
        echo "WARNING: PostgreSQL is up, but migrations might not be fully applied (tables empty or missing)."
    fi
else
    echo "UNKNOWN (psql not installed locally)"
fi

echo "2. Checking sharded Redis nodes..."
for port in "${REDIS_PORTS[@]}"; do
    echo -n "   - Shard on port $port... "
    if which redis-cli >/dev/null 2>&1; then
        PING_RES=$(redis-cli -p "$port" -a "$REDIS_PASSWORD" ping 2>/dev/null || true)
        if [ "$PING_RES" = "PONG" ]; then
            echo "HEALTHY (PONG)"
        else
            echo "FAILED ($PING_RES)"
            exit 1
        fi
    else
        if check_port 127.0.0.1 "$port" "Redis"; then
            echo "HEALTHY (TCP port responsive)"
        else
            echo "FAILED"
            exit 1
        fi
    fi
done

echo -n "3. Checking ClickHouse (HTTP port $CH_HTTP_PORT)... "
CH_HEALTHY=false
if which curl >/dev/null 2>&1; then
    PING_RES=$(curl -s "http://127.0.0.1:$CH_HTTP_PORT/ping" || true)
    if [ "$PING_RES" = "Ok." ]; then
        echo "HEALTHY (HTTP /ping)"
        CH_HEALTHY=true
    fi
fi

if [ "$CH_HEALTHY" = "false" ]; then
    if check_port 127.0.0.1 9000 "ClickHouse Native" || check_port 127.0.0.1 "$CH_HTTP_PORT" "ClickHouse HTTP"; then
        echo "HEALTHY (TCP port responsive)"
    else
        echo "FAILED"
        echo "ERROR: ClickHouse is not running or not accessible."
        exit 1
    fi
fi

echo "SUCCESS: All dependencies are healthy and ready!"
