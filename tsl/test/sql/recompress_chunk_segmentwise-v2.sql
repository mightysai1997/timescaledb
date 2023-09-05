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

-- case1: 1 segment affected
-- Number of segments decompressed is 1
BEGIN;
-- affects segment with sequence_num = 10
INSERT INTO metrics_compressed VALUES ('2000-01-05 20:06:00+05:30'::timestamp with time zone, 1, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-04 23:06:00+05:30'::timestamp with time zone, 1, 2, 3, 222.2, 333.3);
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_2_6_chunk WHERE device_id = 1 ORDER BY 1, 2;
set client_min_messages TO LOG;
CALL recompress_chunk('_timescaledb_internal._hyper_1_1_chunk');
RESET client_min_messages;
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_2_6_chunk WHERE device_id = 1 ORDER BY 1, 2;
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
ROLLBACK;

-- case2: 2 segments affected with same segment by values
-- Number of segments decompressed is 2
BEGIN;
-- affects segment with sequence_num = 20
INSERT INTO metrics_compressed VALUES ('2000-01-03 15:46:00+05:30'::timestamp with time zone, 1, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-03 15:56:00+05:30'::timestamp with time zone, 1, 2, 3, 222.2, 333.3);
-- affects segment with sequence_num = 40
INSERT INTO metrics_compressed VALUES ('2000-01-01 15:30:00+05:30'::timestamp with time zone, 1, 2, 3, 22.2, 33.3);
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_2_6_chunk WHERE device_id = 1 ORDER BY 1, 2;
set client_min_messages TO LOG;
CALL recompress_chunk('_timescaledb_internal._hyper_1_1_chunk');
RESET client_min_messages;
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_2_6_chunk WHERE device_id = 1 ORDER BY 1, 2;
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
ROLLBACK;

-- case3: 2 different segments affected
-- Number of segments decompressed is 2
BEGIN;
-- affects segment with sequence_num = 20
INSERT INTO metrics_compressed VALUES ('2000-01-03 15:46:00+05:30'::timestamp with time zone, 1, 2, 3, 22.2, 33.3);
-- affects segment with sequence_num = 40
INSERT INTO metrics_compressed VALUES ('2000-01-01 15:30:00+05:30'::timestamp with time zone, 3, 2, 3, 222.2, 333.3);
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_2_6_chunk WHERE device_id IN (1, 3) ORDER BY 1, 2;
set client_min_messages TO LOG;
CALL recompress_chunk('_timescaledb_internal._hyper_1_1_chunk');
RESET client_min_messages;
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_2_6_chunk WHERE device_id IN (1, 3) ORDER BY 1, 2;
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
ROLLBACK;

-- case4: 2 segments affected with same segment by values + new segments created
-- Number of segments decompressed is 2
BEGIN;
-- affects segment with sequence_num = 20
INSERT INTO metrics_compressed VALUES ('2000-01-03 15:46:00+05:30'::timestamp with time zone, 1, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-03 15:56:00+05:30'::timestamp with time zone, 1, 2, 3, 222.2, 333.3);
-- affects segment with sequence_num = 40
INSERT INTO metrics_compressed VALUES ('2000-01-01 15:30:00+05:30'::timestamp with time zone, 1, 2, 3, 22.2, 33.3);
-- create new segment
INSERT INTO metrics_compressed VALUES ('2000-01-03 15:46:00+05:30'::timestamp with time zone, 11, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-03 15:56:00+05:30'::timestamp with time zone, 11, 2, 3, 222.2, 333.3);
INSERT INTO metrics_compressed VALUES ('2000-01-03 15:46:00+05:30'::timestamp with time zone, 12, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-01 15:30:00+05:30'::timestamp with time zone, 13, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-01 15:30:00+05:30'::timestamp with time zone, 14, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-01 15:30:00+05:30'::timestamp with time zone, 15, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-01 15:30:00+05:30'::timestamp with time zone, 12, 2, 3, 22.2, 33.3);
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_2_6_chunk ORDER BY 1, 2;
set client_min_messages TO LOG;
CALL recompress_chunk('_timescaledb_internal._hyper_1_1_chunk');
RESET client_min_messages;
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_2_6_chunk ORDER BY 1, 2;
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
ROLLBACK;

-- case5: 3 different segments affected with same segment by values + new segments created
-- Number of segments decompressed is 3
BEGIN;
-- affects segment with sequence_num = 20
INSERT INTO metrics_compressed VALUES ('2000-01-03 15:46:00+05:30'::timestamp with time zone, 1, 2, 3, 2.2, 3.3);
INSERT INTO metrics_compressed VALUES ('2000-01-03 15:56:00+05:30'::timestamp with time zone, 2, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-01 15:30:00+05:30'::timestamp with time zone, 3, 2, 3, 222.2, 333.3);
-- create new segment
INSERT INTO metrics_compressed VALUES ('2000-01-03 15:46:00+05:30'::timestamp with time zone, 11, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-03 15:56:00+05:30'::timestamp with time zone, 11, 2, 3, 222.2, 333.3);
INSERT INTO metrics_compressed VALUES ('2000-01-03 15:46:00+05:30'::timestamp with time zone, 12, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-01 15:30:00+05:30'::timestamp with time zone, 13, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-01 15:30:00+05:30'::timestamp with time zone, 14, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-01 15:30:00+05:30'::timestamp with time zone, 15, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-01 15:30:00+05:30'::timestamp with time zone, 12, 2, 3, 22.2, 33.3);
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_2_6_chunk ORDER BY 1, 2;
set client_min_messages TO LOG;
CALL recompress_chunk('_timescaledb_internal._hyper_1_1_chunk');
RESET client_min_messages;
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_2_6_chunk ORDER BY 1, 2;
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
ROLLBACK;

-- case6: all segments affected
-- Number of segments decompressed is 5
BEGIN;
-- affects segment with sequence_num = 20
INSERT INTO metrics_compressed VALUES ('2000-01-03 15:46:00+05:30'::timestamp with time zone, 1, 2, 3, 2.2, 3.3);
INSERT INTO metrics_compressed VALUES ('2000-01-03 15:56:00+05:30'::timestamp with time zone, 2, 2, 3, 22.2, 33.3);
-- affects segment with sequence_num = 40
INSERT INTO metrics_compressed VALUES ('2000-01-01 15:30:00+05:30'::timestamp with time zone, 3, 2, 3, 222.2, 333.3);
-- affects segment with sequence_num = 10
INSERT INTO metrics_compressed VALUES ('2000-01-04 15:46:00+05:30'::timestamp with time zone, 4, 2, 3, 2222.2, 3333.3);
INSERT INTO metrics_compressed VALUES ('2000-01-05 15:56:00+05:30'::timestamp with time zone, 5, 2, 3, 22222.2, 33333.3);
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_2_6_chunk ORDER BY 1, 2;
set client_min_messages TO LOG;
CALL recompress_chunk('_timescaledb_internal._hyper_1_1_chunk');
RESET client_min_messages;
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_2_6_chunk ORDER BY 1, 2;
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
ROLLBACK;

-- case7: all segments affected + new segments created
-- Number of segments decompressed is 5
BEGIN;
-- affects segment with sequence_num = 10
INSERT INTO metrics_compressed VALUES ('2000-01-05 15:46:00+05:30'::timestamp with time zone, 1, 2, 3, 2.2, 3.3);
INSERT INTO metrics_compressed VALUES ('2000-01-05 15:56:00+05:30'::timestamp with time zone, 2, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-05 15:30:00+05:30'::timestamp with time zone, 3, 2, 3, 222.2, 333.3);
INSERT INTO metrics_compressed VALUES ('2000-01-05 15:46:00+05:30'::timestamp with time zone, 4, 2, 3, 2222.2, 3333.3);
INSERT INTO metrics_compressed VALUES ('2000-01-05 15:56:00+05:30'::timestamp with time zone, 5, 2, 3, 22222.2, 33333.3);
-- create new segment
INSERT INTO metrics_compressed VALUES ('2000-01-03 15:46:00+05:30'::timestamp with time zone, 11, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-03 15:56:00+05:30'::timestamp with time zone, 11, 2, 3, 222.2, 333.3);
INSERT INTO metrics_compressed VALUES ('2000-01-03 15:46:00+05:30'::timestamp with time zone, 11, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-01 15:30:00+05:30'::timestamp with time zone, 13, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-01 15:30:00+05:30'::timestamp with time zone, 14, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-01 15:30:00+05:30'::timestamp with time zone, 15, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-01 15:30:00+05:30'::timestamp with time zone, 12, 2, 3, 22.2, 33.3);
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_2_6_chunk ORDER BY 1, 2;
set client_min_messages TO LOG;
CALL recompress_chunk('_timescaledb_internal._hyper_1_1_chunk');
RESET client_min_messages;
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_2_6_chunk ORDER BY 1, 2;
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
ROLLBACK;

-- case8: create new rows in existing segments
BEGIN;
-- does not affects any segments
INSERT INTO metrics_compressed VALUES ('1999-12-31 05:30:00+05:30'::timestamp with time zone, 1, 2, 3, 2.2, 3.3);
INSERT INTO metrics_compressed VALUES ('1999-12-30 15:30:00+05:30'::timestamp with time zone, 1, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('1999-12-31 05:30:00+05:30'::timestamp with time zone, 2, 2, 3, 222.2, 333.3);
INSERT INTO metrics_compressed VALUES ('1999-12-31 05:30:00+05:30'::timestamp with time zone, 3, 2, 3, 2222.2, 3333.3);
INSERT INTO metrics_compressed VALUES ('1999-12-31 05:30:00+05:30'::timestamp with time zone, 4, 2, 3, 22222.2, 33333.3);
INSERT INTO metrics_compressed VALUES ('1999-12-30 15:30:00+05:30'::timestamp with time zone, 4, 2, 3, 22222.2, 33333.3);
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_2_6_chunk ORDER BY 1, 2;
set client_min_messages TO LOG;
CALL recompress_chunk('_timescaledb_internal._hyper_1_1_chunk');
RESET client_min_messages;
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_2_6_chunk ORDER BY 1, 2;
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
ROLLBACK;

-- case9: check with UPDATE/DELETE on compressed chunks
BEGIN;
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk ORDER BY id;
/* delete device id 4 */
DELETE FROM metrics_compressed WHERE device_id = 4;
/* update device id 3 */
UPDATE metrics_compressed SET v0 = 111, v1 = 222, v2 = 333, v3 = 444 WHERE device_id = 3;
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk ORDER BY id;
-- INSERT new segments with device_id = 4
INSERT INTO metrics_compressed VALUES ('1999-12-31 05:30:00+05:30'::timestamp with time zone, 4, 2, 3, 2.2, 3.3);
INSERT INTO metrics_compressed VALUES ('1999-12-30 15:30:00+05:30'::timestamp with time zone, 4, 2, 3, 22.2, 33.3);
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_2_6_chunk ORDER BY 1, 2;
set client_min_messages TO LOG;
CALL recompress_chunk('_timescaledb_internal._hyper_1_1_chunk');
RESET client_min_messages;
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_2_6_chunk ORDER BY 1, 2;
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk ORDER BY id;
ROLLBACK;

-- case10: gaps between sequence no: are filled up
-- Number of segments decompressed is 2
BEGIN;
-- affects segment with sequence_num = 20,30
INSERT INTO metrics_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, V+1.5,  V + 2.5, V + 0.5, NULL FROM generate_series('2000-01-02 01:26:00+05:30'::timestamptz,'2000-01-04 20:06:00+05:30','22m') gtime(time), generate_series(3,3,1) gdevice(device_id), generate_series(1,100,1) gv(V);
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_2_6_chunk WHERE device_id = 3 ORDER BY 1, 2;
set client_min_messages TO LOG;
CALL recompress_chunk('_timescaledb_internal._hyper_1_1_chunk');
RESET client_min_messages;
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_2_6_chunk WHERE device_id = 3 ORDER BY 1, 2;
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
ROLLBACK;

DROP TABLE metrics_compressed;

-- case11: check with segmentby column being TEXT
SELECT * FROM (
    SELECT 3 priority, 'en_US' "COLLATION"
    UNION ALL (SELECT 2, collname FROM pg_collation WHERE collname ilike 'en_us%' ORDER BY collname limit 1)
    UNION ALL (SELECT 1, collname FROM pg_collation WHERE collname ilike 'en_us_utf%8%' ORDER BY collname limit 1)
) c
ORDER BY priority limit 1 \gset

CREATE TABLE compressed_collation_ht(
    time timestamp,
    name text collate :"COLLATION",
    value float
);

SELECT create_hypertable('compressed_collation_ht', 'time');

ALTER TABLE compressed_collation_ht SET
    (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'name',
        timescaledb.compress_orderby = 'time'
    );

INSERT INTO compressed_collation_ht VALUES
    ('2021-01-01 01:01:01', 'á', '1'),
    ('2021-01-01 01:01:02', 'b', '2'),
    ('2021-01-01 01:01:03', 'ç', '2');

SELECT compress_chunk(i) FROM show_chunks('compressed_collation_ht') i;

BEGIN;
/* below inserts should affect existing chunk and also create new chunks */
INSERT INTO compressed_collation_ht VALUES
    ('2021-01-01 01:01:01', 'á', '1');

INSERT INTO compressed_collation_ht (time, name, value)
SELECT time, 'á', '1'
    FROM
        generate_series(
            '2021-01-01 01:01:01' :: timestamptz,
            '2022-01-05 23:55:00+0',
            '2m'
        ) gtime(time);

-- check metadata
SELECT name, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
    FROM _timescaledb_internal.compress_hyper_4_8_chunk ORDER BY 1, 2;

set client_min_messages TO LOG;
CALL recompress_chunk('_timescaledb_internal._hyper_3_7_chunk');
RESET client_min_messages;

SELECT name, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
    FROM _timescaledb_internal.compress_hyper_4_8_chunk ORDER BY 1, 2;

ROLLBACK;
DROP TABLE compressed_collation_ht;

-- case12: check segmentby columns with NULL values
CREATE TABLE mytab (time TIMESTAMPTZ NOT NULL, a INT, b INT, c INT);
SELECT table_name FROM create_hypertable('mytab', 'time', chunk_time_interval => interval '10 day');
SELECT '2023-10-10 04:33:44.1234+05:30' as start_date \gset
-- set both segmentby columns (a,c) = (value, NULL)
INSERT INTO mytab
    SELECT time,
        CASE WHEN (:'start_date'::timestamptz - time < interval '1 days') THEN 1
             WHEN (:'start_date'::timestamptz - time < interval '2 days') THEN 2
             WHEN (:'start_date'::timestamptz - time < interval '3 days') THEN 3 ELSE 4 END as a
    from generate_series(:'start_date'::timestamptz - interval '30 days', :'start_date'::timestamptz, interval '5 sec') as g1(time);

-- set both segmentby columns (a,c) = (NULL, value)
INSERT INTO mytab
    SELECT time, NULL, 3, 4
    from generate_series(:'start_date'::timestamptz - interval '30 days', :'start_date'::timestamptz, interval '5 sec') as g1(time);

-- set both segmentby columns (a,c) = (NULL, NULL)
INSERT INTO mytab
    SELECT time, NULL, 5, NULL
    from generate_series(:'start_date'::timestamptz - interval '30 days', :'start_date'::timestamptz, interval '5 sec') as g1(time);

-- set both segmentby columns (a,c) = (value, value)
INSERT INTO mytab
    SELECT time, 6, 5, 7
    from generate_series(:'start_date'::timestamptz - interval '30 days', :'start_date'::timestamptz, interval '5 sec') as g1(time);

-- enable compression
ALTER TABLE mytab SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'a, c'
);
-- compress chunks
SELECT compress_chunk(c.schema_name|| '.' || c.table_name)
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.hypertable ht WHERE c.hypertable_id = ht.id and ht.table_name = 'mytab' and c.compressed_chunk_id IS NULL
ORDER BY c.table_name DESC;

-- check metadata
SELECT a, c, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_6_68_chunk ORDER BY 1, 2;

BEGIN;
-- insert new values
-- should affect segments with values (4, NULL)
INSERT INTO mytab VALUES ('2023-09-10 05:00:00.1234+05:30', 4, 8, NULL);
INSERT INTO mytab VALUES ('2023-09-10 05:00:00.1234+05:30', 4, 9, NULL);

-- should affect segments with values (6, 7)
INSERT INTO mytab VALUES ('2023-09-10 05:00:00.1234+05:30', 6, 10, 7);

-- should affect segments with values (NULL, 4)
INSERT INTO mytab VALUES ('2023-09-10 05:00:00.1234+05:30', NULL, 11, 4);
INSERT INTO mytab VALUES ('2023-09-10 05:00:00.1234+05:30', NULL, 12, 4);
INSERT INTO mytab VALUES ('2023-09-10 05:00:00.1234+05:30', NULL, 13, 4);

-- should affect segments with values (NULL, NULL)
INSERT INTO mytab VALUES ('2023-09-10 05:00:00.1234+05:30', NULL, 14, NULL);
INSERT INTO mytab VALUES ('2023-09-10 05:00:00.1234+05:30', NULL, 15, NULL);
INSERT INTO mytab VALUES ('2023-09-10 05:00:00.1234+05:30', NULL, 16, NULL);
INSERT INTO mytab VALUES ('2023-09-10 05:00:00.1234+05:30', NULL, 17, NULL);

set client_min_messages TO LOG;
CALL recompress_chunk('_timescaledb_internal._hyper_5_61_chunk');
RESET client_min_messages;

-- check metadata after recompression
SELECT a, c, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_6_68_chunk ORDER BY 1, 2;

ROLLBACK;
DROP TABLE mytab;

-- case13: check that new segmentby values are spread all over
-- compressed rows
CREATE TABLE mytab (time TIMESTAMPTZ NOT NULL, a INT);
SELECT table_name FROM create_hypertable('mytab', 'time', chunk_time_interval => interval '10 day');
SELECT '2023-10-10 04:33:44.1234+05:30' as start_date \gset

-- insert segmentby values in the range of 4 - 8
INSERT INTO mytab
    SELECT time, a_val
    from generate_series(:'start_date'::timestamptz - interval '30 days', :'start_date'::timestamptz, interval '50m') as g1(time), generate_series(4,8, 1) as g2(a_val);

-- insert segmentby values in the range of 12 - 19
INSERT INTO mytab
    SELECT time, a_val
    from generate_series(:'start_date'::timestamptz - interval '30 days', :'start_date'::timestamptz, interval '50m') as g1(time), generate_series(12,19, 1) as g2(a_val);

-- insert segmentby values in the range of 32 - 34
INSERT INTO mytab
    SELECT time, a_val
    from generate_series(:'start_date'::timestamptz - interval '30 days', :'start_date'::timestamptz, interval '50m') as g1(time), generate_series(32,34, 1) as g2(a_val);

-- enable compression
ALTER TABLE mytab SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'a'
);
-- compress chunks
SELECT compress_chunk(c.schema_name|| '.' || c.table_name)
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.hypertable ht WHERE c.hypertable_id = ht.id and ht.table_name = 'mytab' and c.compressed_chunk_id IS NULL
ORDER BY c.table_name DESC;

BEGIN;
-- insert segmentby values in the range of 0 - 3
INSERT INTO mytab
    SELECT time, a_val
    from generate_series(:'start_date'::timestamptz - interval '30 days', :'start_date'::timestamptz, interval '58m') as g1(time), generate_series(0,3, 1) as g2(a_val);

-- insert segmentby values in the range of 9 - 10
INSERT INTO mytab
    SELECT time, a_val
    from generate_series(:'start_date'::timestamptz - interval '30 days', :'start_date'::timestamptz, interval '65m') as g1(time), generate_series(9,10, 1) as g2(a_val);

-- insert segmentby values in the range of 22 - 30
INSERT INTO mytab
    SELECT time, a_val
    from generate_series(:'start_date'::timestamptz - interval '30 days', :'start_date'::timestamptz, interval '50m') as g1(time), generate_series(22,30, 1) as g2(a_val);

-- insert segmentby values in the range of 40 - 50
INSERT INTO mytab
    SELECT time, a_val
    from generate_series(:'start_date'::timestamptz - interval '30 days', :'start_date'::timestamptz, interval '50m') as g1(time), generate_series(40,50, 1) as g2(a_val);

SELECT a, _ts_meta_sequence_num, _ts_meta_count FROM _timescaledb_internal.compress_hyper_8_76_chunk ORDER BY 1,2,3;
set client_min_messages TO LOG;
CALL recompress_chunk('_timescaledb_internal._hyper_7_69_chunk');
RESET client_min_messages;
SELECT a, _ts_meta_sequence_num, _ts_meta_count FROM _timescaledb_internal.compress_hyper_8_76_chunk ORDER BY 1,2,3;

ROLLBACK;
DROP TABLE mytab;
