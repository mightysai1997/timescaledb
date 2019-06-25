--telemetry tests that require a community license

SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'num_continuous_aggs');
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'num_hypertables');

-- check telemetry picks up flagged content from metadata
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'db_metadata');

-- check timescaledb_telemetry.cloud
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'instance_metadata');


--create a continuous agg
CREATE TABLE device_readings (
      observation_time  TIMESTAMPTZ       NOT NULL
);
SELECT table_name FROM create_hypertable('device_readings', 'observation_time');
CREATE VIEW device_summary
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', observation_time) as bucket,
  min(observation_time)
FROM
  device_readings
GROUP BY bucket;

SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'num_continuous_aggs');
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'num_hypertables');
