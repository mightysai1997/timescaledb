-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO errors

TRUNCATE TABLE compressed;
SELECT ts_compress_table(:'DATA_IN'::REGCLASS, 'compressed'::REGCLASS, :'COMPRESSION_INFO'::_timescaledb_catalog.hypertable_compression[]);

--test that decompression gives same result in forward direction
WITH original AS (
  SELECT * FROM :DATA_OUT ORDER BY device, time
),
decompressed AS (
  SELECT * FROM (SELECT :DECOMPRESS_FORWARD_CMD FROM compressed ORDER BY device) AS q
)
SELECT 'Number of rows different between original and decompressed forward (expect 0)', count(*)
FROM original
FULL OUTER JOIN decompressed ON (original.device = decompressed.device AND original.time = decompressed.t)
WHERE (original.*) IS DISTINCT FROM (decompressed.*);

with original AS (
  SELECT * FROM :DATA_OUT AS q ORDER BY device, time
),
decompressed AS (
  SELECT * FROM (SELECT :DECOMPRESS_FORWARD_CMD FROM compressed) AS q
)
SELECT *
FROM original
FULL OUTER JOIN decompressed ON (original.device = decompressed.device AND original.time = decompressed.t)
WHERE (original.*) IS DISTINCT FROM (decompressed.*);

\set ECHO all
