-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License,
-- see LICENSE-APACHE at the top level directory.

CREATE OR REPLACE FUNCTION assert_true(
    val boolean
)
 RETURNS VOID LANGUAGE PLPGSQL IMMUTABLE AS
$BODY$
BEGIN
    IF !val THEN
        RAISE 'Assert failed';
    END IF;
END
$BODY$;


CREATE OR REPLACE FUNCTION assert_equal(
    val1 anyelement,
    val2 anyelement
)
 RETURNS VOID LANGUAGE PLPGSQL IMMUTABLE AS
$BODY$
BEGIN
    IF val1 != val2 THEN
        RAISE 'Assert failed';
    END IF;
END
$BODY$;

\c single :ROLE_SUPERUSER

CREATE OR REPLACE FUNCTION allow_downgrade_to_apache()
RETURNS VOID
AS :MODULE_PATHNAME, 'ts_allow_downgrade_to_apache'
LANGUAGE C;

\c single :ROLE_DEFAULT_PERM_USER
