--keep same order of tables as tables.sql
SELECT * FROM _timescaledb_catalog.hypertable ORDER BY id;
SELECT * FROM _timescaledb_catalog.tablespace ORDER BY id;
SELECT * FROM _timescaledb_catalog.dimension ORDER BY id;
SELECT * FROM _timescaledb_catalog.dimension_slice ORDER BY id;
SELECT * FROM _timescaledb_catalog.chunk ORDER BY id;
SELECT * FROM _timescaledb_catalog.chunk_constraint ORDER BY chunk_id, dimension_slice_id, constraint_name;
--index name changed, so don't output that
SELECT hypertable_index_name, hypertable_id, chunk_id FROM _timescaledb_catalog.chunk_index ORDER BY  hypertable_index_name, hypertable_id, chunk_id;
