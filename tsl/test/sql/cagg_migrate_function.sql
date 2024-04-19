-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Prepare a hypertable
CREATE TABLE temperature (
  time timestamptz NOT NULL,
  sensor int,
  value float
);

SELECT create_hypertable('temperature', 'time');

INSERT INTO temperature
  SELECT time, sensor_id, sensor_id * sensor_id + 100
    FROM generate_series('2000-01-01 0:00:00+0'::timestamptz, '2000-01-01 23:59:59+0','1m') g1(time),
    generate_series(1, 2, 1) AS g2(sensor_id);


-- Create a CAgg using time_bucket_ng
SET timescaledb.debug_allow_cagg_with_deprecated_funcs = true;

CREATE MATERIALIZED VIEW cagg_1_ng
  WITH  (timescaledb.continuous) AS
  SELECT timescaledb_experimental.time_bucket_ng('5 minutes', time, 'Europe/Berlin') as time, sensor, avg(value) AS avg
    FROM temperature
    GROUP BY 1,2
    WITH NO DATA;

-- Get the name of the direct and partial view
SELECT format('%I.%I', partial_view_schema, partial_view_name)::regclass AS partial_view,
       format('%I.%I', direct_view_schema, direct_view_name)::regclass AS direct_view
  FROM _timescaledb_catalog.continuous_agg where user_view_name = 'cagg_1_ng'
  \gset

SELECT * FROM _timescaledb_catalog.continuous_aggs_bucket_function;
SELECT pg_get_viewdef(:'partial_view', true);
SELECT pg_get_viewdef(:'direct_view', true);
SELECT pg_get_viewdef('cagg_1_ng', true);

CALL cagg_migrate_to_time_bucket('cagg_1_ng');

SELECT * FROM _timescaledb_catalog.continuous_aggs_bucket_function;
SELECT pg_get_viewdef(:'partial_view', true);
SELECT pg_get_viewdef(:'direct_view', true);
SELECT pg_get_viewdef('cagg_1_ng', true);

-- Create a real-time CAgg using time_bucket_ng
CREATE MATERIALIZED VIEW cagg_1_ng_rt
  WITH  (timescaledb.continuous, timescaledb.materialized_only=false) AS
  SELECT timescaledb_experimental.time_bucket_ng('5 minutes', time, 'Europe/Berlin') as time, sensor, avg(value) AS avg
    FROM temperature
    GROUP BY 1,2
    WITH NO DATA;

SELECT format('%I.%I', partial_view_schema, partial_view_name)::regclass AS partial_view,
       format('%I.%I', direct_view_schema, direct_view_name)::regclass AS direct_view
  FROM _timescaledb_catalog.continuous_agg where user_view_name = 'cagg_1_ng_rt'
  \gset

SELECT * FROM _timescaledb_catalog.continuous_aggs_bucket_function;
SELECT pg_get_viewdef(:'partial_view', true);
SELECT pg_get_viewdef(:'direct_view', true);
SELECT pg_get_viewdef('cagg_1_ng_rt', true);

CALL cagg_migrate_to_time_bucket('cagg_1_ng_rt');

SELECT * FROM _timescaledb_catalog.continuous_aggs_bucket_function;
SELECT pg_get_viewdef(:'partial_view', true);
SELECT pg_get_viewdef(:'direct_view', true);
SELECT pg_get_viewdef('cagg_1_ng_rt', true);
