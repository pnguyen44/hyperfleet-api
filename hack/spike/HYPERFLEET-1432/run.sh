#!/usr/bin/env bash
# Run the spike POC: schema → roles → test-ops
set -euo pipefail

cd "$(dirname "$0")"
source ./dev-env.sh

echo "=== Creating schema ==="
psql -v ON_ERROR_STOP=1 -f ops/schema.sql

echo "=== Applying role grants ==="
psql -v ON_ERROR_STOP=1 -f ops/roles.sql

echo "=== Running store ops + role tests (some permission denied errors expected) ==="
psql -v ON_ERROR_STOP=1 -f ops/test-ops.sql
