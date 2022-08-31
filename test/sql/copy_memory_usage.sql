-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Test that transaction memory usage with COPY doesn't grow.
-- We need memory usage in PortalContext after the completion of the query, so
-- we'll have to log it from a trigger that runs after the query is completed.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

create table uk_price_paid(price integer, "date" date, postcode1 text, postcode2 text, type smallint, is_new bool, duration smallint, addr1 text, addr2 text, street text, locality text, town text, district text, country text, category smallint);
-- Aim to about 100 partitions, the data is from 1995 to 2022.
select create_hypertable('uk_price_paid', 'date', chunk_time_interval => interval '90 day');

-- This is where we log the memory usage.
create table portal_memory_log(id serial, bytes int);

-- Returns the amount of memory currently allocated in a given
-- memory context. Only works for PortalContext, and doesn't work for PG 12.
create or replace function ts_debug_allocated_bytes(text) returns int
    as :MODULE_PATHNAME, 'ts_debug_allocated_bytes'
    language c strict volatile;

-- Log current memory usage into the log table.
create function log_memory() returns trigger as $$
    begin
        insert into portal_memory_log
            values (default, (select ts_debug_allocated_bytes('PortalContext')));
        return new;
    end;
$$ language plpgsql;

-- Add a trigger that runs after completion of each INSERT/COPY and logs the
-- current memory usage.
create trigger check_update after insert on uk_price_paid
    for each statement execute function log_memory();

-- Memory leaks often happen on cache invalidation, so make sure they are
-- invalidated often and independently (at co-prime periods).
set timescaledb.max_open_chunks_per_insert = 2;
set timescaledb.max_cached_chunks_per_hypertable = 3;

-- Try increasingly larger data sets by concatenating the same file multiple
-- times.
\copy uk_price_paid from program 'bash -c "cat <(zcat < data/prices-10k-random-1.tsv.gz)"';
\copy uk_price_paid from program 'bash -c "cat <(zcat < data/prices-10k-random-1.tsv.gz) <(zcat < data/prices-10k-random-1.tsv.gz)"';
\copy uk_price_paid from program 'bash -c "cat <(zcat < data/prices-10k-random-1.tsv.gz) <(zcat < data/prices-10k-random-1.tsv.gz) <(zcat < data/prices-10k-random-1.tsv.gz)"';
\copy uk_price_paid from program 'bash -c "cat <(zcat < data/prices-10k-random-1.tsv.gz) <(zcat < data/prices-10k-random-1.tsv.gz) <(zcat < data/prices-10k-random-1.tsv.gz) <(zcat < data/prices-10k-random-1.tsv.gz)"';
\copy uk_price_paid from program 'bash -c "cat <(zcat < data/prices-10k-random-1.tsv.gz) <(zcat < data/prices-10k-random-1.tsv.gz) <(zcat < data/prices-10k-random-1.tsv.gz) <(zcat < data/prices-10k-random-1.tsv.gz) <(zcat < data/prices-10k-random-1.tsv.gz)"';

select count(*) from portal_memory_log;

-- We'll only compare the biggest runs, because the smaller ones have variance
-- due to new chunks being created and other unknown reasons. Allow 5% change of
-- memory usage to account for some randomness.
select * from portal_memory_log where (
    select (max(bytes) - min(bytes)) / max(bytes)::float > 0.05
    from portal_memory_log where id >= 4
);