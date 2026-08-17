-- pgbench script: one read + one CAS write per transaction (generic desire store traffic)
\set cluster random(1, 100)
\set desire random(1, 10)
SELECT version FROM desire_store.desires
WHERE partition_key = 'cluster-' || :cluster AND desire_key = 'cluster-' || :cluster || '/ns/desire-' || :desire
\gset cur_
UPDATE desire_store.desires
SET status = jsonb_build_object('phase', 'Applied'), version = version + 1
WHERE partition_key = 'cluster-' || :cluster AND desire_key = 'cluster-' || :cluster || '/ns/desire-' || :desire
  AND version = :cur_version;
