-- seed data for load test
-- 100 partitions (clusters) x 10 desires = 1000 rows

INSERT INTO desire_store.desires (partition_key, desire_key, kind, spec, status, version)
SELECT
    'cluster-' || c,
    'cluster-' || c || '/ns/desire-' || d,
    'apply',
    jsonb_build_object('manifest', jsonb_build_object('kind', 'ConfigMap', 'seq', d)),
    NULL,
    0
FROM generate_series(1, 100) AS c
CROSS JOIN generate_series(1, 10) AS d
ON CONFLICT DO NOTHING;
