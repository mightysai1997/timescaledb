-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License,
-- see LICENSE-APACHE at the top level directory.

\c :TEST_DBNAME

--list all extension functions in public schema
SELECT DISTINCT proname
FROM pg_proc
WHERE OID IN (
    SELECT objid
    FROM pg_catalog.pg_depend                                                                                                               WHERE   refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass AND
    refobjid = (select oid from pg_extension where extname='timescaledb') AND
    deptype = 'e' and classid = 'pg_catalog.pg_proc'::regclass
) AND pronamespace = 'public'::regnamespace
ORDER BY proname;
