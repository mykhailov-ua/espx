#!/usr/bin/env bash
# Smoke checks for local docker-compose / host-network stack.
# Usage: ./scripts/smoke-local.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

TRACKER_PORT="${SERVER_PORT:-8181}"
PROCESSOR_PORT="${PROCESSOR_PORT:-8186}"
EDGE_PORT="${EDGE_PORT:-8180}"
REDIS_PORT="${REDIS_PORT:-6479}"
REDIS_PASSWORD="${REDIS_PASSWORD:-}"

pass=0
fail=0

check() {
  local name="$1"
  shift
  if "$@"; then
    echo "PASS  $name"
    pass=$((pass + 1))
  else
    echo "FAIL  $name"
    fail=$((fail + 1))
  fi
}

http_code() {
  curl -sf -o /dev/null -w '%{http_code}' "$1"
}

echo "eSPX local smoke"

check "tracker /health (${TRACKER_PORT})" test "$(http_code "http://127.0.0.1:${TRACKER_PORT}/health")" = "200"
check "processor /health (${PROCESSOR_PORT})" test "$(http_code "http://127.0.0.1:${PROCESSOR_PORT}/health")" = "200"

if command -v redis-cli >/dev/null 2>&1 && [[ -n "${REDIS_PASSWORD}" ]]; then
  check "redis ping (:${REDIS_PORT})" redis-cli -p "${REDIS_PORT}" -a "${REDIS_PASSWORD}" ping 2>/dev/null | grep -q PONG
  check "redis AOF enabled" redis-cli -p "${REDIS_PORT}" -a "${REDIS_PASSWORD}" INFO persistence 2>/dev/null | grep -q 'aof_enabled:1'
else
  echo "SKIP  redis checks (redis-cli or REDIS_PASSWORD missing)"
fi

if [[ -f deploy/geoip/GeoLite2-Country.mmdb ]]; then
  echo "INFO  GeoLite2 mmdb present"
else
  echo "WARN  deploy/geoip/GeoLite2-Country.mmdb missing (OK when ENV=development)"
fi

echo "pass=${pass} fail=${fail}"
if [[ "$fail" -gt 0 ]]; then
  exit 1
fi
