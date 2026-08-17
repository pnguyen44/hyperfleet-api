-- store ops + role enforcement
-- Expect permission denied errors in the role tests at the end.

BEGIN;

INSERT INTO desire_store.desires (partition_key, desire_key, kind, spec, status, version)
VALUES
    ('cluster-a', 'cluster-a/ns/default', 'apply', '{"manifest": {"kind": "Namespace"}}'::jsonb, NULL, 0),
    ('cluster-a', 'cluster-a/ns/default/read', 'read', '{"paired": true}'::jsonb, NULL, 0),
    ('cluster-a', 'cluster-a/cm/config', 'apply', '{"manifest": {"kind": "ConfigMap"}}'::jsonb, NULL, 0),
    ('cluster-b', 'cluster-b/ns/default', 'apply', '{"manifest": {"kind": "Namespace"}}'::jsonb, NULL, 0);

\echo '--- Partition listing: expect only cluster-a rows (applier polls one cluster) ---'
SELECT desire_key, kind FROM desire_store.desires WHERE partition_key = 'cluster-a' ORDER BY desire_key;

\echo '--- CAS: expect UPDATE 1 (version matches) ---'
UPDATE desire_store.desires
SET status = '{"phase": "Applied"}'::jsonb, version = version + 1
WHERE partition_key = 'cluster-a' AND desire_key = 'cluster-a/ns/default' AND version = 0;

\echo '--- CAS conflict: expect UPDATE 0 (stale version rejected) ---'
UPDATE desire_store.desires
SET status = '{"phase": "Conflict"}'::jsonb, version = version + 1
WHERE partition_key = 'cluster-a' AND desire_key = 'cluster-a/ns/default' AND version = 0;

SELECT desire_key, status, version
FROM desire_store.desires
WHERE partition_key = 'cluster-a' AND desire_key = 'cluster-a/ns/default';

COMMIT;

\echo ''
\echo '--- Role enforcement: allowed paths, expect success ---'

SET ROLE adapter_role;
INSERT INTO desire_store.desires (partition_key, desire_key, kind, spec, version)
VALUES ('cluster-a', 'cluster-a/cm/role-test', 'apply', '{"manifest": {"kind": "ConfigMap"}}'::jsonb, 0);
RESET ROLE;

SET ROLE applier_role;
UPDATE desire_store.desires
SET status = '{"phase": "Applied"}'::jsonb, version = version + 1
WHERE partition_key = 'cluster-a' AND desire_key = 'cluster-a/cm/role-test' AND version = 0;
RESET ROLE;

SELECT desire_key, spec, status, version
FROM desire_store.desires
WHERE partition_key = 'cluster-a' AND desire_key = 'cluster-a/cm/role-test';

\echo '--- Adapter prefix delete: expect DELETE 2 (cm/config + cm/role-test) ---'
SET ROLE adapter_role;
DELETE FROM desire_store.desires
WHERE partition_key = 'cluster-a' AND desire_key LIKE 'cluster-a/cm/%';
RESET ROLE;

\echo '--- Row count after delete: expect 2 (ns/default + ns/default/read) ---'
SELECT COUNT(*) AS cluster_a_rows FROM desire_store.desires WHERE partition_key = 'cluster-a';

-- Turn ON_ERROR_STOP off for just this block so these expected failures don't
-- abort the script, regardless of how it's invoked (see README).
\set ON_ERROR_STOP off

\echo ''
\echo '--- Role enforcement: forbidden paths below, expect permission denied for each ---'

\echo '--- Adapter cannot write status via UPDATE ---'
SET ROLE adapter_role;
UPDATE desire_store.desires
SET status = '{"phase": "Hijack"}'::jsonb, version = version + 1
WHERE partition_key = 'cluster-a' AND desire_key = 'cluster-a/ns/default/read';
RESET ROLE;

\echo '--- Adapter cannot write status via INSERT ---'
SET ROLE adapter_role;
INSERT INTO desire_store.desires (partition_key, desire_key, kind, spec, status, version)
VALUES ('cluster-a', 'cluster-a/cm/role-test-insert', 'apply', '{}'::jsonb, '{"phase": "Hijack"}'::jsonb, 0);
RESET ROLE;

\echo '--- Applier cannot write spec via UPDATE ---'
SET ROLE applier_role;
UPDATE desire_store.desires
SET spec = '{"manifest": {"kind": "Hijack"}}'::jsonb, version = version + 1
WHERE partition_key = 'cluster-a' AND desire_key = 'cluster-a/ns/default/read';
RESET ROLE;

\echo '--- Applier cannot delete ---'
SET ROLE applier_role;
DELETE FROM desire_store.desires
WHERE partition_key = 'cluster-a' AND desire_key = 'cluster-a/ns/default/read';
RESET ROLE;

\set ON_ERROR_STOP on

\echo ''
\echo '=== test-ops.sql complete. The 4 permission denied errors above are expected. ==='
