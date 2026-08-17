# HYPERFLEET-1432 spike: Postgres JSONB desire store POC

Spike ticket - [HYPERFLEET-1432](https://redhat.atlassian.net/browse/HYPERFLEET-1432).

Spike doc: [desire-store-api-postgres-spike.md](https://github.com/openshift-hyperfleet/architecture/blob/main/hyperfleet/docs/desire-store-api-postgres-spike.md)

## Table of Contents

- [Prereqs](#prereqs)
- [Run](#run)
- [Load test](#load-test)
  - [In-cluster](#in-cluster)
  - [Headroom test (with API traffic)](#headroom-test-with-api-traffic)
  - [Via port-forward (quick smoke test only)](#via-port-forward-quick-smoke-test-only)
- [Cleanup](#cleanup)

## Prereqs

- `make db/setup` from this repo
- `psql`, `pgbench` (load test only)

Scripts default to dev Postgres credentials from `make db/setup` (`dev-env.sh`). Override with env vars if needed.

## Run

From the `hyperfleet-api` repo root

```bash
sh hack/spike/HYPERFLEET-1432/run.sh
```

`run.sh` runs `ops/schema.sql` → `ops/roles.sql` → `ops/test-ops.sql`. `schema.sql` drops any previous run first, so safe to re-run.

`ops/test-ops.sql` ends with role tests; some `permission denied` errors are expected.

## Load test

Local Docker Postgres doesn't reflect real headroom, so this runs against your GCP dev cluster's Postgres pod instead.

Ensure your personal dev cluster is set up with `hyperfleet-api` deployed.

From `hyperfleet-infra`:

```bash
make install-api
kubectl get svc -n hyperfleet hyperfleet-api-postgresql   # confirm Postgres exists
```

### In-cluster

`kubectl port-forward` adds tunnel latency that dominates the result (see spike doc), so run `pgbench` from inside the cluster instead, same network path a real adapter/applier would use.

```bash
# 1. Launch a throwaway pod with psql/pgbench
kubectl run pgbench-runner -n hyperfleet --image=postgres:14 --restart=Never --command -- sleep 3600

# 2. Copy the spike SQL files into it
kubectl cp hack/spike/HYPERFLEET-1432/ops hyperfleet/pgbench-runner:/tmp/ops
kubectl cp hack/spike/HYPERFLEET-1432/load-test hyperfleet/pgbench-runner:/tmp/load-test

# 3. Run load test (echo labels are in load-test/run.sh)
kubectl exec -n hyperfleet pgbench-runner -- bash -c '
export PGHOST=hyperfleet-api-postgresql PGPORT=5432 PGUSER=hyperfleet PGPASSWORD=hyperfleet-dev-password PGDATABASE=hyperfleet
sh /tmp/load-test/run.sh
'

# 4. Clean up
kubectl delete pod pgbench-runner -n hyperfleet
```

### Headroom test (with API traffic)

To measure headroom against the API's own load (see spike doc "Limitation"), run
`api-traffic.sh` concurrently with the pgbench load test.

The `postgres:14` image does not include `curl`, so API traffic runs from a separate
`curlimages/curl` pod (same cluster network as the API):

```bash
# 1. Launch a curl pod (keep pgbench-runner running from In-cluster above)
kubectl run api-traffic-runner -n hyperfleet --image=curlimages/curl:latest --restart=Never --command -- sleep 3600
kubectl wait -n hyperfleet --for=condition=Ready pod/api-traffic-runner --timeout=120s

kubectl cp hack/spike/HYPERFLEET-1432/load-test/api-traffic.sh hyperfleet/api-traffic-runner:/tmp/api-traffic.sh

# 2. Start load test in terminal 1 (pgbench-runner)
kubectl exec -n hyperfleet pgbench-runner -- bash -c '
export PGHOST=hyperfleet-api-postgresql PGPORT=5432 PGUSER=hyperfleet PGPASSWORD=hyperfleet-dev-password PGDATABASE=hyperfleet
sh /tmp/load-test/run.sh
'

# 3. Start API traffic in terminal 2 at roughly the same time (api-traffic-runner)
kubectl exec -n hyperfleet api-traffic-runner -- sh -c '
export API_URL=http://hyperfleet-api:8000 DURATION=30
sh /tmp/api-traffic.sh
'
```

Compare pgbench tps to the desire-only run. A lower tps means API traffic is consuming
headroom that desires would otherwise use.

```bash
# 4. Clean up curl pod when done
kubectl delete pod api-traffic-runner -n hyperfleet
```

### Via port-forward (quick smoke test only)

Not representative, tunnel latency dominates the result, see spike doc. Useful only to confirm the scripts run end to end.

Stop the local container first, it also binds port 5432 and will block the port-forward:

```bash
docker stop psql-hyperfleet
```

Port-forward (kubectl context should already be your dev cluster after infra setup):

```bash
kubectl port-forward -n hyperfleet svc/hyperfleet-api-postgresql 5432:5432
```

In another terminal, override the credentials (chart defaults, not `dev-env.sh`'s):

```bash
PGUSER=hyperfleet PGPASSWORD=hyperfleet-dev-password PGDATABASE=hyperfleet \
  sh hack/spike/HYPERFLEET-1432/load-test/run.sh
```

## Cleanup

Optional — from the repo root:

```bash
source hack/spike/HYPERFLEET-1432/dev-env.sh
psql -c "DROP SCHEMA IF EXISTS desire_store CASCADE; DROP ROLE IF EXISTS adapter_role; DROP ROLE IF EXISTS applier_role;"
```
