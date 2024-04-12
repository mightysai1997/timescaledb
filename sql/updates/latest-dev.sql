-- Remove multi-node CAGG support
DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_cagg_log_add_entry(integer,bigint,bigint);
DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_hyper_log_add_entry(integer,bigint,bigint);
DROP FUNCTION IF EXISTS _timescaledb_internal.materialization_invalidation_log_delete(integer);
DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_process_cagg_log(integer,integer,regtype,bigint,bigint,integer[],bigint[],bigint[]);
DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_process_cagg_log(integer,integer,regtype,bigint,bigint,integer[],bigint[],bigint[],text[]);
DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_process_hypertable_log(integer,integer,regtype,integer[],bigint[],bigint[]);
DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_process_hypertable_log(integer,integer,regtype,integer[],bigint[],bigint[],text[]);
DROP FUNCTION IF EXISTS _timescaledb_internal.hypertable_invalidation_log_delete(integer);

DROP FUNCTION IF EXISTS _timescaledb_functions.invalidation_cagg_log_add_entry(integer,bigint,bigint);
DROP FUNCTION IF EXISTS _timescaledb_functions.invalidation_hyper_log_add_entry(integer,bigint,bigint);
DROP FUNCTION IF EXISTS _timescaledb_functions.materialization_invalidation_log_delete(integer);
DROP FUNCTION IF EXISTS _timescaledb_functions.invalidation_process_cagg_log(integer,integer,regtype,bigint,bigint,integer[],bigint[],bigint[]);
DROP FUNCTION IF EXISTS _timescaledb_functions.invalidation_process_cagg_log(integer,integer,regtype,bigint,bigint,integer[],bigint[],bigint[],text[]);
DROP FUNCTION IF EXISTS _timescaledb_functions.invalidation_process_hypertable_log(integer,integer,regtype,integer[],bigint[],bigint[]);
DROP FUNCTION IF EXISTS _timescaledb_functions.invalidation_process_hypertable_log(integer,integer,regtype,integer[],bigint[],bigint[],text[]);
DROP FUNCTION IF EXISTS _timescaledb_functions.hypertable_invalidation_log_delete(integer);

-- Remove chunk metadata when marked as dropped
CREATE FUNCTION _timescaledb_functions.remove_dropped_chunk_metadata(_hypertable_id INTEGER)
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
  _chunk_id INTEGER;
  _removed INTEGER := 0;
BEGIN
  FOR _chunk_id IN
    SELECT id FROM _timescaledb_catalog.chunk
    WHERE hypertable_id = _hypertable_id
    AND dropped IS TRUE
    AND NOT EXISTS (
        SELECT FROM information_schema.tables
        WHERE tables.table_schema = chunk.schema_name
        AND tables.table_name = chunk.table_name
    )
    AND NOT EXISTS (
        SELECT FROM _timescaledb_catalog.hypertable
        JOIN _timescaledb_catalog.continuous_agg ON continuous_agg.raw_hypertable_id = hypertable.id
        WHERE hypertable.id = chunk.hypertable_id
        -- for the old caggs format we need to keep chunk metadata for dropped chunks
        AND continuous_agg.finalized IS FALSE
    )
  LOOP
    _removed := _removed + 1;
    RAISE INFO 'Removing metadata of chunk % from hypertable %', _chunk_id, _hypertable_id;
    WITH _dimension_slice_remove AS (
        DELETE FROM _timescaledb_catalog.dimension_slice
        USING _timescaledb_catalog.chunk_constraint
        WHERE dimension_slice.id = chunk_constraint.dimension_slice_id
        AND chunk_constraint.chunk_id = _chunk_id
        RETURNING _timescaledb_catalog.dimension_slice.id
    )
    DELETE FROM _timescaledb_catalog.chunk_constraint
    USING _dimension_slice_remove
    WHERE chunk_constraint.dimension_slice_id = _dimension_slice_remove.id;

    DELETE FROM _timescaledb_internal.bgw_policy_chunk_stats
    WHERE bgw_policy_chunk_stats.chunk_id = _chunk_id;

    DELETE FROM _timescaledb_catalog.chunk_index
    WHERE chunk_index.chunk_id = _chunk_id;

    DELETE FROM _timescaledb_catalog.compression_chunk_size
    WHERE compression_chunk_size.chunk_id = _chunk_id
    OR compression_chunk_size.compressed_chunk_id = _chunk_id;

    DELETE FROM _timescaledb_catalog.chunk
    WHERE chunk.id = _chunk_id
    OR chunk.compressed_chunk_id = _chunk_id;
  END LOOP;

  RETURN _removed;
END;
$$ SET search_path TO pg_catalog, pg_temp;

SELECT _timescaledb_functions.remove_dropped_chunk_metadata(id) AS chunks_metadata_removed
FROM _timescaledb_catalog.hypertable;

--
-- Rebuild the catalog table `_timescaledb_catalog.continuous_aggs_bucket_function`
--

CREATE OR REPLACE FUNCTION _timescaledb_functions.cagg_get_bucket_function(
    mat_hypertable_id INTEGER
) RETURNS regprocedure AS '@MODULE_PATHNAME@', 'ts_continuous_agg_get_bucket_function' LANGUAGE C STRICT VOLATILE;

-- Since we need now the regclass of the used bucket function, we have to recover it
-- by parsing the view query by calling 'cagg_get_bucket_function'.
CREATE TABLE _timescaledb_catalog._tmp_continuous_aggs_bucket_function AS
    SELECT
      mat_hypertable_id,
      _timescaledb_functions.cagg_get_bucket_function(mat_hypertable_id),
      bucket_width,
      origin,
      NULL::text AS bucket_offset,
      timezone,
      false AS bucket_fixed_width
    FROM
      _timescaledb_catalog.continuous_aggs_bucket_function
    ORDER BY
         mat_hypertable_id;

ALTER EXTENSION timescaledb
    DROP TABLE _timescaledb_catalog.continuous_aggs_bucket_function;

DROP TABLE _timescaledb_catalog.continuous_aggs_bucket_function;

CREATE TABLE _timescaledb_catalog.continuous_aggs_bucket_function (
  mat_hypertable_id integer NOT NULL,
  -- The bucket function
  bucket_func regprocedure NOT NULL,
  -- `bucket_width` argument of the function, e.g. "1 month"
  bucket_width text NOT NULL,
  -- optional `origin` argument of the function provided by the user
  bucket_origin text,
  -- optional `offset` argument of the function provided by the user
  bucket_offset text,
  -- optional `timezone` argument of the function provided by the user
  bucket_timezone text,
  -- fixed or variable sized bucket
  bucket_fixed_width bool NOT NULL,
  -- table constraints
  CONSTRAINT continuous_aggs_bucket_function_pkey PRIMARY KEY (mat_hypertable_id),
  CONSTRAINT continuous_aggs_bucket_function_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE
);

INSERT INTO _timescaledb_catalog.continuous_aggs_bucket_function
  SELECT * FROM _timescaledb_catalog._tmp_continuous_aggs_bucket_function;

DROP TABLE _timescaledb_catalog._tmp_continuous_aggs_bucket_function;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_bucket_function', '');

GRANT SELECT ON TABLE _timescaledb_catalog.continuous_aggs_bucket_function TO PUBLIC;

ANALYZE _timescaledb_catalog.continuous_aggs_bucket_function;

ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_functions.cagg_get_bucket_function(INTEGER);
DROP FUNCTION IF EXISTS _timescaledb_functions.cagg_get_bucket_function(INTEGER);

--
-- End rebuild the catalog table `_timescaledb_catalog.continuous_aggs_bucket_function`
--

-- Convert _timescaledb_catalog.continuous_aggs_bucket_function.bucket_origin to TimestampTZ
UPDATE _timescaledb_catalog.continuous_aggs_bucket_function
   SET bucket_origin = bucket_origin::timestamp::timestamptz::text
   WHERE length(bucket_origin) > 1;

-- Historically, we have used empty strings for undefined bucket_origin and timezone
-- attributes. This is now replaced by proper NULL values. We use TRIM() to ensure we handle empty string well.
UPDATE _timescaledb_catalog.continuous_aggs_bucket_function SET bucket_origin = NULL WHERE TRIM(bucket_origin) = '';
UPDATE _timescaledb_catalog.continuous_aggs_bucket_function SET bucket_timezone = NULL WHERE TRIM(bucket_timezone) = '';

-- So far, there were no difference between 0 and -1 retries. Since now on, 0 means no retries. Updating the retry
-- count of existing jobs to -1 to keep the current semantics.
UPDATE _timescaledb_config.bgw_job SET max_retries = -1 WHERE max_retries = 0;

DROP FUNCTION IF EXISTS _timescaledb_functions.get_chunk_relstats;
DROP FUNCTION IF EXISTS _timescaledb_functions.get_chunk_colstats;
DROP FUNCTION IF EXISTS _timescaledb_internal.get_chunk_relstats;
DROP FUNCTION IF EXISTS _timescaledb_internal.get_chunk_colstats;

-- In older TSDB versions, we disabled autovacuum for compressed chunks
-- to keep the statistics. However, this restriction was removed in
-- #5118 but no migration was performed to remove the custom
-- autovacuum setting for existing chunks.
DO $$
DECLARE
  chunk regclass;
BEGIN
  FOR chunk IN
    SELECT pg_catalog.format('%I.%I', schema_name, table_name)::regclass
      FROM _timescaledb_catalog.chunk c
      JOIN pg_catalog.pg_class AS pc ON (pc.oid=format('%I.%I', schema_name, table_name)::regclass)
      CROSS JOIN unnest(reloptions) AS u(option)
      WHERE
        dropped = false
        AND osm_chunk = false
        AND option LIKE 'autovacuum_enabled%'
  LOOP
    EXECUTE pg_catalog.format('ALTER TABLE %s RESET (autovacuum_enabled);', chunk::text);
  END LOOP;
END
$$;

--
-- Rebuild the catalog table `_timescaledb_catalog.continuous_agg`
--

-- (1) Create missing entries in _timescaledb_catalog.continuous_aggs_bucket_function
CREATE OR REPLACE FUNCTION _timescaledb_functions.cagg_get_bucket_function(
    mat_hypertable_id INTEGER
) RETURNS regprocedure AS '@MODULE_PATHNAME@', 'ts_continuous_agg_get_bucket_function' LANGUAGE C STRICT VOLATILE;

-- Make sure function points to the new version of TSDB
CREATE OR REPLACE FUNCTION _timescaledb_functions.to_interval(unixtime_us BIGINT) RETURNS INTERVAL
    AS '@MODULE_PATHNAME@', 'ts_pg_unix_microseconds_to_interval' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- We need to create entries in continuous_aggs_bucket_function for all CAggs that were treated so far
-- as fixed indicated by a bucket_width != -1
INSERT INTO _timescaledb_catalog.continuous_aggs_bucket_function
  SELECT
  mat_hypertable_id,
  _timescaledb_functions.cagg_get_bucket_function(mat_hypertable_id),
  -- Intervals needs to be converted into the proper interval format
  -- Function name could be prefixed with 'public.'. Therefore LIKE instead of starts_with is used
  CASE WHEN _timescaledb_functions.cagg_get_bucket_function(mat_hypertable_id)::text LIKE '%time_bucket(interval,%' THEN
    _timescaledb_functions.to_interval(bucket_width)::text
  ELSE
    bucket_width::text
  END,
  NULL, -- bucket_origin
  NULL, -- bucket_offset
  NULL, -- bucket_timezone
  true  -- bucket_fixed_width
  FROM _timescaledb_catalog.continuous_agg WHERE bucket_width != -1;

ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_functions.cagg_get_bucket_function(INTEGER);
DROP FUNCTION IF EXISTS _timescaledb_functions.cagg_get_bucket_function(INTEGER);

-- (2) Rebuild catalog table
DROP VIEW IF EXISTS timescaledb_experimental.policies;
DROP VIEW IF EXISTS timescaledb_information.hypertables;
DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;

DROP PROCEDURE IF EXISTS @extschema@.cagg_migrate (REGCLASS, BOOLEAN, BOOLEAN);
DROP FUNCTION IF EXISTS _timescaledb_internal.cagg_migrate_pre_validation (TEXT, TEXT, TEXT);
DROP FUNCTION IF EXISTS _timescaledb_functions.cagg_migrate_pre_validation (TEXT, TEXT, TEXT);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_create_plan (_timescaledb_catalog.continuous_agg, TEXT, BOOLEAN, BOOLEAN);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_create_plan (_timescaledb_catalog.continuous_agg, TEXT, BOOLEAN, BOOLEAN);

DROP FUNCTION IF EXISTS _timescaledb_functions.cagg_migrate_plan_exists (INTEGER);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_plan (_timescaledb_catalog.continuous_agg);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_plan (_timescaledb_catalog.continuous_agg);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_create_new_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_create_new_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_disable_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_disable_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_enable_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_enable_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_copy_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_copy_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_refresh_new_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_refresh_new_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_copy_data (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_copy_data (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_override_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_override_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_drop_old_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_drop_old_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);

ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    DROP CONSTRAINT continuous_aggs_materialization_invalid_materialization_id_fkey;

ALTER TABLE _timescaledb_catalog.continuous_aggs_watermark
    DROP CONSTRAINT continuous_aggs_watermark_mat_hypertable_id_fkey;

ALTER EXTENSION timescaledb
    DROP TABLE _timescaledb_catalog.continuous_agg;

CREATE TABLE _timescaledb_catalog._tmp_continuous_agg AS
    SELECT
        mat_hypertable_id,
        raw_hypertable_id,
        parent_mat_hypertable_id,
        user_view_schema,
        user_view_name,
        partial_view_schema,
        partial_view_name,
        direct_view_schema,
        direct_view_name,
        materialized_only,
        finalized
    FROM
        _timescaledb_catalog.continuous_agg
    ORDER BY
        mat_hypertable_id;

DROP TABLE _timescaledb_catalog.continuous_agg;

CREATE TABLE _timescaledb_catalog.continuous_agg (
    mat_hypertable_id integer NOT NULL,
    raw_hypertable_id integer NOT NULL,
    parent_mat_hypertable_id integer,
    user_view_schema name NOT NULL,
    user_view_name name NOT NULL,
    partial_view_schema name NOT NULL,
    partial_view_name name NOT NULL,
    direct_view_schema name NOT NULL,
    direct_view_name name NOT NULL,
    materialized_only bool NOT NULL DEFAULT FALSE,
    finalized bool NOT NULL DEFAULT TRUE,
    -- table constraints
    CONSTRAINT continuous_agg_pkey PRIMARY KEY (mat_hypertable_id),
    CONSTRAINT continuous_agg_partial_view_schema_partial_view_name_key UNIQUE (partial_view_schema, partial_view_name),
    CONSTRAINT continuous_agg_user_view_schema_user_view_name_key UNIQUE (user_view_schema, user_view_name),
    CONSTRAINT continuous_agg_mat_hypertable_id_fkey
        FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
    CONSTRAINT continuous_agg_raw_hypertable_id_fkey
        FOREIGN KEY (raw_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
    CONSTRAINT continuous_agg_parent_mat_hypertable_id_fkey
        FOREIGN KEY (parent_mat_hypertable_id)
        REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE
);

INSERT INTO _timescaledb_catalog.continuous_agg
SELECT * FROM _timescaledb_catalog._tmp_continuous_agg;
DROP TABLE _timescaledb_catalog._tmp_continuous_agg;

CREATE INDEX continuous_agg_raw_hypertable_id_idx ON _timescaledb_catalog.continuous_agg (raw_hypertable_id);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_agg', '');

GRANT SELECT ON TABLE _timescaledb_catalog.continuous_agg TO PUBLIC;

ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    ADD CONSTRAINT continuous_aggs_materialization_invalid_materialization_id_fkey
        FOREIGN KEY (materialization_id)
        REFERENCES _timescaledb_catalog.continuous_agg(mat_hypertable_id) ON DELETE CASCADE;

ALTER TABLE _timescaledb_catalog.continuous_aggs_watermark
    ADD CONSTRAINT continuous_aggs_watermark_mat_hypertable_id_fkey
        FOREIGN KEY (mat_hypertable_id)
        REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE;

ANALYZE _timescaledb_catalog.continuous_agg;

--
-- END Rebuild the catalog table `_timescaledb_catalog.continuous_agg`
--

--
-- START bgw_job_stat_history
--
DROP VIEW IF EXISTS timescaledb_information.job_errors;

CREATE SEQUENCE _timescaledb_internal.bgw_job_stat_history_id_seq MINVALUE 1;

CREATE TABLE _timescaledb_internal.bgw_job_stat_history (
  id INTEGER NOT NULL DEFAULT nextval('_timescaledb_internal.bgw_job_stat_history_id_seq'),
  job_id INTEGER NOT NULL,
  pid INTEGER,
  execution_start TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  execution_finish TIMESTAMPTZ,
  succeeded boolean NOT NULL DEFAULT FALSE,
  config jsonb,
  error_data jsonb,
  -- table constraints
  CONSTRAINT bgw_job_stat_history_pkey PRIMARY KEY (id)
);

ALTER SEQUENCE _timescaledb_internal.bgw_job_stat_history_id_seq OWNED BY _timescaledb_internal.bgw_job_stat_history.id;

CREATE INDEX bgw_job_stat_history_job_id_idx ON _timescaledb_internal.bgw_job_stat_history (job_id);

REVOKE ALL ON _timescaledb_internal.bgw_job_stat_history FROM PUBLIC;

INSERT INTO _timescaledb_internal.bgw_job_stat_history (job_id, pid, execution_start, execution_finish, error_data)
SELECT
  job_id,
  pid,
  start_time,
  finish_time,
  error_data
FROM
  _timescaledb_internal.job_errors
ORDER BY
  job_id, start_time;

ALTER EXTENSION timescaledb
    DROP TABLE _timescaledb_internal.job_errors;

DROP TABLE _timescaledb_internal.job_errors;

UPDATE
  _timescaledb_config.bgw_job
SET
  application_name = 'Job History Log Retention Policy [2]',
  proc_schema = '_timescaledb_functions',
  proc_name = 'policy_job_stat_history_retention',
  check_schema = '_timescaledb_functions',
  check_name = 'policy_job_stat_history_retention_check'
WHERE
  id = 2;

DROP FUNCTION IF EXISTS _timescaledb_internal.policy_job_error_retention(job_id integer,config jsonb);
DROP FUNCTION IF EXISTS _timescaledb_internal.policy_job_error_retention_check(config jsonb);
DROP FUNCTION IF EXISTS _timescaledb_functions.policy_job_error_retention(job_id integer,config jsonb);
DROP FUNCTION IF EXISTS _timescaledb_functions.policy_job_error_retention_check(config jsonb);

--
-- END bgw_job_stat_history
--

DROP FUNCTION IF EXISTS @extschema@.add_dimension(
    REGCLASS,
    NAME,
    INTEGER,
    ANYELEMENT,
    REGPROC,
    BOOLEAN
);

CREATE FUNCTION @extschema@.add_dimension(
    hypertable              REGCLASS,
    column_name             NAME,
    number_partitions       INTEGER = NULL,
    chunk_time_interval     ANYELEMENT = NULL::BIGINT,
    partitioning_func       REGPROC = NULL,
    if_not_exists           BOOLEAN = FALSE,
    correlated              BOOLEAN = FALSE
) RETURNS TABLE(dimension_id INT, schema_name NAME, table_name NAME, column_name NAME, created BOOL)
AS '@MODULE_PATHNAME@', 'ts_dimension_add' LANGUAGE C VOLATILE;


--
-- Rebuild the catalog table `_timescaledb_catalog.dimension with type column
--

CREATE TABLE _timescaledb_internal._tmp_dimension
AS SELECT * from _timescaledb_catalog.dimension;

CREATE TABLE _timescaledb_internal.tmp_dimension_seq_value AS
SELECT last_value, is_called FROM _timescaledb_catalog.dimension_id_seq;

--drop foreign keys on dimension table
ALTER TABLE _timescaledb_catalog.dimension_slice DROP CONSTRAINT
dimension_slice_dimension_id_fkey;

--drop dependent views
DROP VIEW IF EXISTS timescaledb_information.chunks;
DROP VIEW IF EXISTS timescaledb_information.dimensions;
DROP VIEW IF EXISTS timescaledb_information.hypertable_compression_settings;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.dimension;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.dimension_id_seq;
DROP TABLE _timescaledb_catalog.dimension;

CREATE TABLE _timescaledb_catalog.dimension (
  id serial NOT NULL ,
  hypertable_id integer NOT NULL,
  column_name name NOT NULL,
  column_type REGTYPE NOT NULL,
  aligned boolean NOT NULL,
  -- closed dimensions
  num_slices smallint NULL,
  partitioning_func_schema name NULL,
  partitioning_func name NULL,
  -- open dimensions (e.g., time)
  interval_length bigint NULL,
  -- compress interval is used by rollup procedure during compression
  -- in order to merge multiple chunks into a single one
  compress_interval_length bigint NULL,
  integer_now_func_schema name NULL,
  integer_now_func name NULL,
  type "char",
  -- table constraints
  CONSTRAINT dimension_pkey PRIMARY KEY (id),
  CONSTRAINT dimension_hypertable_id_column_name_key UNIQUE (hypertable_id, column_name),
  CONSTRAINT dimension_check CHECK ((partitioning_func_schema IS NULL AND partitioning_func IS NULL) OR (partitioning_func_schema IS NOT NULL AND partitioning_func IS NOT NULL)),
  CONSTRAINT dimension_check1 CHECK ((num_slices IS NULL AND interval_length IS NOT NULL) OR (num_slices IS NOT NULL AND interval_length IS NULL)),
  CONSTRAINT dimension_check2 CHECK ((integer_now_func_schema IS NULL AND integer_now_func IS NULL) OR (integer_now_func_schema IS NOT NULL AND integer_now_func IS NOT NULL)),
  CONSTRAINT dimension_interval_length_check CHECK (interval_length IS NULL OR interval_length > 0 OR type = 'C'),
  CONSTRAINT dimension_compress_interval_length_check CHECK (compress_interval_length IS NULL OR compress_interval_length > 0),
  CONSTRAINT dimension_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE
);

INSERT INTO _timescaledb_catalog.dimension
( id, hypertable_id, column_name, column_type,
  aligned, num_slices, partitioning_func_schema,
  partitioning_func, interval_length,
  compress_interval_length,
  integer_now_func_schema, integer_now_func,
  type)
SELECT id, hypertable_id, column_name, column_type,
  aligned, num_slices, partitioning_func_schema,
  partitioning_func, interval_length,
  compress_interval_length,
  integer_now_func_schema, integer_now_func,
  CASE WHEN interval_length IS NULL AND num_slices IS NOT NULL THEN
        'c' -- closed dimension
       WHEN interval_length IS NOT NULL AND num_slices is NULL THEN
        'o' -- open dimension
       ELSE
        'a' -- any.. This should never happen
  END as type
FROM _timescaledb_internal._tmp_dimension;

-- Check that there's no entry with type == 'a'
DO $$
DECLARE
  count_var INTEGER;
BEGIN
    SELECT count(*) FROM _timescaledb_catalog.dimension INTO count_var WHERE
        type = 'a';
    IF count_var != 0 THEN
        RAISE EXCEPTION 'invalid dimension entry found!';
    END IF;
END
$$;

ALTER SEQUENCE _timescaledb_catalog.dimension_id_seq OWNED BY _timescaledb_catalog.dimension.id;
SELECT setval('_timescaledb_catalog.dimension_id_seq', last_value, is_called) FROM _timescaledb_internal.tmp_dimension_seq_value;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.dimension', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.dimension', 'id'), '');

--add the foreign key constraints
ALTER TABLE _timescaledb_catalog.dimension_slice ADD CONSTRAINT
dimension_slice_dimension_id_fkey FOREIGN KEY (dimension_id)
REFERENCES _timescaledb_catalog.dimension(id) ON DELETE CASCADE;

--cleanup
DROP TABLE _timescaledb_internal._tmp_dimension;
DROP TABLE _timescaledb_internal.tmp_dimension_seq_value;

GRANT SELECT ON _timescaledb_catalog.dimension_id_seq TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.dimension TO PUBLIC;

-- end recreate _timescaledb_catalog.dimension table --
