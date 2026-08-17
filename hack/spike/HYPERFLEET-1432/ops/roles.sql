-- Goal: enforce adapter→spec and applier→status at the DB layer (not just in Go code)
-- Run as superuser after ops/schema.sql

-- Two permission bundles for the two writers on the desire store
CREATE ROLE adapter_role NOLOGIN;
CREATE ROLE applier_role NOLOGIN;

-- Both roles need access to the desire_store schema
GRANT USAGE ON SCHEMA desire_store TO adapter_role, applier_role;

-- Adapter owns desires: creates, updates spec, deletes; cannot write status
GRANT SELECT, INSERT (partition_key, desire_key, kind, spec, version)
    ON desire_store.desires TO adapter_role;
GRANT UPDATE (spec, version)
    ON desire_store.desires TO adapter_role;
GRANT DELETE ON desire_store.desires TO adapter_role;

-- Applier reports reconciliation results: updates status only; cannot write spec
GRANT SELECT ON desire_store.desires TO applier_role;
GRANT UPDATE (status, version)
    ON desire_store.desires TO applier_role;

-- Lets test-ops.sql impersonate each role with SET ROLE
GRANT adapter_role TO CURRENT_USER;
GRANT applier_role TO CURRENT_USER;
