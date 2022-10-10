-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test distributed COPY with text/binary format for input and for data transfer
-- to data nodes.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

\set DN_DBNAME_1 :TEST_DBNAME _1
\set DN_DBNAME_2 :TEST_DBNAME _2
\set DN_DBNAME_3 :TEST_DBNAME _3

SELECT 1 FROM add_data_node('data_node_1', host => 'localhost',
                            database => :'DN_DBNAME_1');
SELECT 1 FROM add_data_node('data_node_2', host => 'localhost',
                            database => :'DN_DBNAME_2');
SELECT 1 FROM add_data_node('data_node_3', host => 'localhost',
                            database => :'DN_DBNAME_3');
GRANT USAGE ON FOREIGN SERVER data_node_1, data_node_2, data_node_3 TO PUBLIC;

SET ROLE :ROLE_1;


-- Aim to about 100 partitions, the data is from 1995 to 2022.
create table uk_price_paid(price integer, "date" date, postcode1 text, postcode2 text, type smallint, is_new bool, duration smallint, addr1 text, addr2 text, street text, locality text, town text, district text, country text, category smallint);
select create_distributed_hypertable('uk_price_paid', 'date', 'postcode2',
    chunk_time_interval => interval '90 day');

-- Populate.
\copy uk_price_paid from program 'zcat < data/prices-100k-random-1.tsv.gz';
select count(*), sum(price) from uk_price_paid;

-- Make binary file.
\copy (select * from uk_price_paid) to 'prices-100k.pgbinary' with (format binary);

-- Binary input with binary data transfer.
set timescaledb.enable_connection_binary_data = true;
set timescaledb.dist_copy_transfer_format = 'binary';
create table uk_price_paid_bin(like uk_price_paid);
select create_distributed_hypertable('uk_price_paid_bin', 'date', 'postcode2',
    chunk_time_interval => interval '90 day', replication_factor => 2);

\copy uk_price_paid_bin from 'prices-100k.pgbinary' with (format binary);
select count(*), sum(price) from uk_price_paid_bin;

-- Text input with explicit format option and binary data transfer.
truncate uk_price_paid_bin;
\copy uk_price_paid_bin from program 'zcat < data/prices-100k-random-1.tsv.gz' with (format text);
select count(*), sum(price) from uk_price_paid_bin;

-- Binary input with text data transfer. Doesn't work.
set timescaledb.dist_copy_transfer_format = 'text';
\set ON_ERROR_STOP off
\copy uk_price_paid_bin from 'prices-100k.pgbinary' with (format binary);
\set ON_ERROR_STOP on

-- Text input with text data transfer.
truncate uk_price_paid_bin;
\copy uk_price_paid_bin from program 'zcat < data/prices-100k-random-1.tsv.gz';
select count(*), sum(price) from uk_price_paid_bin;

-- Nonsensical settings
set timescaledb.dist_copy_transfer_format = 'binary';
set timescaledb.enable_connection_binary_data = false;
\set ON_ERROR_STOP off
\copy uk_price_paid_bin from 'prices-100k.pgbinary' with (format binary);
\set ON_ERROR_STOP on

-- Teardown
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
DROP DATABASE :DN_DBNAME_1;
DROP DATABASE :DN_DBNAME_2;
DROP DATABASE :DN_DBNAME_3;
