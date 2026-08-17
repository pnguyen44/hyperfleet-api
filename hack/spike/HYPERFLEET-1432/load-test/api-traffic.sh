#!/usr/bin/env bash
# Generate API read traffic for the duration of the desire-store load test.
# Run this alongside load-test/run.sh and compare pgbench tps with vs without it.
set -euo pipefail

API_URL="${API_URL:-http://hyperfleet-api:8000}"
DURATION="${DURATION:-30}"

if ! command -v curl >/dev/null 2>&1; then
  echo "curl not found in this container. Use a curl image pod (see README Headroom test)."
  exit 1
fi

echo "=== Generating API traffic against $API_URL for ${DURATION}s ==="

start=$(date +%s)
end=$((start + DURATION))
count=0
errors=0

while [ "$(date +%s)" -lt "$end" ]; do
  if curl -sf -o /dev/null "${API_URL}/api/hyperfleet/v1/clusters?page=1&size=10"; then
    count=$((count + 1))
  else
    errors=$((errors + 1))
  fi
  if curl -sf -o /dev/null "${API_URL}/api/hyperfleet/v1/nodepools?page=1&size=10"; then
    count=$((count + 1))
  else
    errors=$((errors + 1))
  fi
done

echo "=== API traffic done: $count requests, $errors errors ==="
