-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- We compare information(\d+) about manually created tables with the ones that were recreated using deparse_table command.
-- There should be no diff.

\set ECHO errors

SELECT format('%s/results/deparse_create.out', :'TEST_OUTPUT_DIR') AS "CREATE_OUT",
       format('%s/results/deparse_recreate.out', :'TEST_OUTPUT_DIR') AS "RECREATE_OUT"
\gset
SELECT format('\! diff %s %s', :'CREATE_OUT', :'RECREATE_OUT') as "DIFF_CMD"
\gset

\ir include/deparse_create.sql

\o :CREATE_OUT
\d+ "public".*

\ir include/deparse_recreate.sql
\o :RECREATE_OUT
\d+ "public".*

\o

:DIFF_CMD

SELECT 'DONE'
