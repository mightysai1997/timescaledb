# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# Test that concurrent queries and recompress_chunk cannot
# deadlock.
#
# This can occur if recompressing since a recompress will first do a
# decompression of the compressed chunk into the uncompressed chunk.
#
# We should only have a single chunk here, otherwise we cannot lock
# the right chunk.
#
# We have fetched the actual recompress procedure from the policy and
# re-implement it here to be able to test that it works as
# expected. Please check policy_internal.sql to see the code it is
# based on.
#
setup {
    CREATE TABLE hyper (time TIMESTAMP NOT NULL, a DOUBLE PRECISION NULL, b DOUBLE PRECISION NULL);

    CREATE INDEX "time_plain" ON hyper(time DESC, a);

    SELECT * FROM create_hypertable('hyper', 'time');

    INSERT INTO hyper
    SELECT to_timestamp(ser), ser, ser+10000 FROM generate_series(0,1000) ser;

    ALTER TABLE hyper SET (timescaledb.compress = true);

    SELECT count(*) FROM (SELECT compress_chunk(show_chunks('hyper'))) i;
}

teardown {
    DROP TABLE hyper;
}

# We are not using a debug point when locking inside recompress_chunk
# after decompress since it is an PL/SQL procedure. Instead we lock
# the compressed chunk, which will block the actual drop in the
# recompress procedure at the right point (before the commit), and we
# can then start the query, which will queue after the others.
session "locks"
setup {
  BEGIN;
}
step "lock_after_decompress" {
    SELECT count(*) FROM show_chunks('hyper');
    DO $$
    DECLARE
      chunk regclass;
    BEGIN
      SELECT format('%I.%I', c.schema_name, c.table_name)::regclass INTO chunk
      FROM show_chunks('hyper'), _timescaledb_catalog.chunk p, _timescaledb_catalog.chunk c
      WHERE p.compressed_chunk_id = c.id
      AND format('%I.%I', p.schema_name, p.table_name)::regclass = show_chunks;
      EXECUTE format('LOCK TABLE %s IN EXCLUSIVE MODE', chunk);
    END;
    $$;
}
step "unlock_after_decompress" {
    ROLLBACK;
}

# When building a simple relation for the query (using
# `build_simple_rel`), it first takes a lock on the relation, and then
# on any indexes for the relation, if they exist (inside
# `get_relation_info`, to get statistics for the relation).
session "query"
step "query_start" {
    SELECT count(*) FROM hyper;
}

# This session will compress all chunks. We should only have a single
# one in this case, so that we block just before reindexing the chunk.
session "recompress"
step "recompress_insert_rows" {
    INSERT INTO hyper
    SELECT to_timestamp(ser), ser, ser+100 FROM generate_series(0,100) ser;
}
step "recompress_chunks_start" {
    DO $$
    DECLARE
      chunk regclass;
    BEGIN
      SELECT show_chunks('hyper') INTO chunk;
      CALL recompress_chunk(chunk);
    END;
    $$;
}
		      
# Since the locking order is different, we locking before reindex we should one one lock on the index and
# one lock on the chunk from different processes.
permutation recompress_insert_rows lock_after_decompress recompress_chunks_start query_start(recompress_chunks_start) unlock_after_decompress

