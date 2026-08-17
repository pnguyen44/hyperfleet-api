#!/usr/bin/env bash
# Load test: schema + seed + pgbench (does not run role tests)
set -euo pipefail

cd "$(dirname "$0")"
# Local runs: dev-env.sh defaults. In-cluster: export PG* before invoking this script.
if [ -f ../dev-env.sh ]; then
  # shellcheck source=../dev-env.sh
  source ../dev-env.sh
fi

echo "=== Resetting schema ==="
psql -v ON_ERROR_STOP=1 -f ../ops/schema.sql

echo "=== Seeding 1000 rows (100 clusters x 10 desires) ==="
psql -v ON_ERROR_STOP=1 -f seed.sql

echo "=== Running pgbench for 30s (target: worst-case ~20,000 writes/sec, see spike doc) ==="
pgbench -U "$PGUSER" -n -f load-test.sql -c 10 -j 4 -T 30 -P 5 "$PGDATABASE"
