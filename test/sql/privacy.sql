\c single :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_privacy() RETURNS VOID
    AS :MODULE_PATHNAME, 'test_privacy' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
\c single :ROLE_DEFAULT_PERM_USER

SHOW timescaledb.telemetry_level;
SET timescaledb.telemetry_level=off;
SHOW timescaledb.telemetry_level;
SELECT _timescaledb_internal.test_privacy();
-- To make sure nothing was sent, we check the UUID table to make sure no exported UUID row was created
SELECT key from _timescaledb_catalog.installation_metadata;
