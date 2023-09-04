-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE metrics_compressed(filler_1 int, filler_2 int, filler_3 int, time timestamptz NOT NULL, device_id int, v0 int, v1 int, v2 float, v3 float);
CREATE INDEX ON metrics_compressed(time);
CREATE INDEX ON metrics_compressed(device_id,time);
SELECT create_hypertable('metrics_compressed','time',create_default_indexes:=false);

ALTER TABLE metrics_compressed DROP COLUMN filler_1;
INSERT INTO metrics_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1,  device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,'2000-01-05 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ALTER TABLE metrics_compressed DROP COLUMN filler_2;
INSERT INTO metrics_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id-1, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-06 0:00:00+0'::timestamptz,'2000-01-12 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ALTER TABLE metrics_compressed DROP COLUMN filler_3;
INSERT INTO metrics_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-13 0:00:00+0'::timestamptz,'2000-01-19 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ANALYZE metrics_compressed;

-- compress chunks
ALTER TABLE metrics_compressed SET (timescaledb.compress, timescaledb.compress_orderby='time DESC', timescaledb.compress_segmentby='device_id');
SELECT compress_chunk(c.schema_name|| '.' || c.table_name)
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.hypertable ht WHERE c.hypertable_id = ht.id and ht.table_name = 'metrics_compressed' and c.compressed_chunk_id IS NULL
ORDER BY c.table_name DESC;

-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;

-- case12: no: of affected existing segment is > 1000, which calls resize_affectedsegmentscxt()
INSERT INTO metrics_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, V+1.5,  V + 2.5, V + 0.5, NULL FROM generate_series('2000-01-02 01:26:00+05:30'::timestamptz,'2000-01-04 20:06:00+05:30','22m') gtime(time), generate_series(1,1020,1) gdevice(device_id), generate_series(1,1,1) gv(V);
CALL recompress_chunk('_timescaledb_internal._hyper_1_1_chunk');
BEGIN;
-- affects segment with sequence_num = 20,30
INSERT INTO metrics_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, V+1.5,  V + 2.5, V + 0.5, NULL FROM generate_series('2000-01-02 01:26:00+05:30'::timestamptz,'2000-01-04 20:06:00+05:30','22m') gtime(time), generate_series(1,1020,1) gdevice(device_id), generate_series(1,1,1) gv(V);
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
SELECT count(distinct device_id) FROM _timescaledb_internal.compress_hyper_2_6_chunk;
CALL recompress_chunk('_timescaledb_internal._hyper_1_1_chunk');
SELECT count(distinct device_id) FROM _timescaledb_internal.compress_hyper_2_6_chunk;
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
ROLLBACK;

DROP TABLE metrics_compressed;
