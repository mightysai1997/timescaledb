-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_uuid() RETURNS UUID
    AS :MODULE_PATHNAME, 'ts_test_uuid' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_exported_uuid() RETURNS UUID
    AS :MODULE_PATHNAME, 'ts_test_exported_uuid' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_install_timestamp() RETURNS TIMESTAMPTZ
    AS :MODULE_PATHNAME, 'ts_test_install_timestamp' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
INSERT INTO _timescaledb_catalog.metadata (key, value, include_in_telemetry) SELECT 'metadata_test', 'FOO', TRUE;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- uuid and install_timestamp should already be in the table before we generate
SELECT COUNT(*) from _timescaledb_catalog.metadata;

SELECT _timescaledb_internal.test_uuid() as uuid_1 \gset
SELECT _timescaledb_internal.test_exported_uuid() as uuid_ex_1 \gset
SELECT _timescaledb_internal.test_install_timestamp() as timestamp_1 \gset

-- Check that there is exactly 1 UUID row
SELECT COUNT(*) from _timescaledb_catalog.metadata where key='uuid';

-- Check that exported_uuid and timestamp are also generated
SELECT COUNT(*) from _timescaledb_catalog.metadata where key='exported_uuid';
SELECT COUNT(*) from _timescaledb_catalog.metadata where key='install_timestamp';

-- Make sure that the UUID is idempotent
SELECT _timescaledb_internal.test_uuid() = :'uuid_1' as uuids_equal;
SELECT _timescaledb_internal.test_uuid() = :'uuid_1' as uuids_equal;
-- Also make sure install_time and exported_uuid are idempotent
SELECT _timescaledb_internal.test_exported_uuid() = :'uuid_ex_1' as exported_uuids_equal;
SELECT _timescaledb_internal.test_exported_uuid() = :'uuid_ex_1' as exported_uuids_equal;
SELECT _timescaledb_internal.test_install_timestamp() = :'timestamp_1' as timestamps_equal;
SELECT _timescaledb_internal.test_install_timestamp() = :'timestamp_1' as timestamps_equal;

-- Now make sure that only the exported_uuid is exported on pg_dump
\c postgres :ROLE_SUPERUSER

\setenv PGOPTIONS '--client-min-messages=warning'
\! utils/pg_dump_aux_dump.sh dump/instmeta.sql
ALTER DATABASE :TEST_DBNAME SET timescaledb.restoring='on';
-- Redirect to /dev/null to suppress NOTICE
\! utils/pg_dump_aux_restore.sh dump/instmeta.sql
ALTER DATABASE :TEST_DBNAME SET timescaledb.restoring='off';

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- Should have all 3 row, because pg_dump includes the insertion of uuid and timestamp.
SELECT COUNT(*) FROM _timescaledb_catalog.metadata;
-- Verify that this is the old exported_uuid
SELECT _timescaledb_internal.test_exported_uuid() = :'uuid_ex_1' as exported_uuids_equal;
-- Verify that the uuid is new
SELECT _timescaledb_internal.test_uuid() = :'uuid_1' as exported_uuids_diff;
-- Verify that the install_timestamp got restored
SELECT _timescaledb_internal.test_install_timestamp() = :'timestamp_1' as timestamps_equal;
SELECT * FROM _timescaledb_catalog.metadata WHERE key = 'metadata_test';

-- check metadata version matches expected value
SELECT x.extversion = m.value AS "version match"
FROM pg_extension x
JOIN _timescaledb_catalog.metadata m ON m.key='timescaledb_version'
WHERE x.extname='timescaledb';

-- test version check in post_restore
\c :TEST_DBNAME :ROLE_SUPERUSER
UPDATE _timescaledb_catalog.metadata SET value = '1.2.3' WHERE key = 'timescaledb_version';
\set ON_ERROR_STOP 0
-- set verbosity to sqlstate to suppress version dependant error message
\set VERBOSITY sqlstate
SELECT timescaledb_post_restore();
\set ON_ERROR_STOP 1

UPDATE _timescaledb_catalog.metadata m SET value = x.extversion FROM pg_extension x WHERE m.key = 'timescaledb_version' AND x.extname='timescaledb';
SELECT timescaledb_post_restore();


