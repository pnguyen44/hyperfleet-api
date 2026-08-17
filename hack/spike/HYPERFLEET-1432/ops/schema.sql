-- desire store table on JSONB
-- Drops any previous spike run so this script is safe to re-run.

DROP SCHEMA IF EXISTS desire_store CASCADE;
DROP ROLE IF EXISTS adapter_role;
DROP ROLE IF EXISTS applier_role;

CREATE SCHEMA desire_store;

CREATE TABLE desire_store.desires (
    partition_key text NOT NULL,
    desire_key text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('apply', 'delete', 'read')),
    spec jsonb,
    status jsonb,
    version integer NOT NULL DEFAULT 0,
    PRIMARY KEY (partition_key, desire_key)
);

-- Applier polls one partition; adapter prefix delete scans partition + key prefix
CREATE INDEX desires_partition_idx ON desire_store.desires (partition_key);
CREATE INDEX desires_partition_key_prefix_idx ON desire_store.desires (partition_key, desire_key text_pattern_ops);
