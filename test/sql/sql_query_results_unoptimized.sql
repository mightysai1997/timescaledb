\o /dev/null
\ir include/create_single_db.sql
\o

SET timescaledb.disable_optimizations= 'on';
\ir include/sql_query_results.sql
