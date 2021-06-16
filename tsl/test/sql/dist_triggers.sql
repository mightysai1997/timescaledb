-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

\unset ECHO
\o /dev/null
\ir include/filter_exec.sql
\ir include/remote_exec.sql
\o
\set ECHO all

\set DATA_NODE_1 :TEST_DBNAME _1
\set DATA_NODE_2 :TEST_DBNAME _2
\set DATA_NODE_3 :TEST_DBNAME _3
\set TABLESPACE_1 :TEST_DBNAME _1
\set TABLESPACE_2 :TEST_DBNAME _2
SELECT
    test.make_tablespace_path(:'TEST_TABLESPACE1_PREFIX', :'TEST_DBNAME') AS spc1path,
    test.make_tablespace_path(:'TEST_TABLESPACE2_PREFIX', :'TEST_DBNAME') AS spc2path
\gset

SELECT (add_data_node (name, host => 'localhost', DATABASE => name)).*
FROM (VALUES (:'DATA_NODE_1'), (:'DATA_NODE_2'), (:'DATA_NODE_3')) v (name);

GRANT USAGE ON FOREIGN SERVER :DATA_NODE_1, :DATA_NODE_2, :DATA_NODE_3 TO PUBLIC;

-- Import testsupport.sql file to data nodes
\unset ECHO
\o /dev/null
\c :DATA_NODE_1
SET client_min_messages TO ERROR;
\ir :TEST_SUPPORT_FILE
\c :DATA_NODE_2
SET client_min_messages TO ERROR;
\ir :TEST_SUPPORT_FILE
\c :DATA_NODE_3
SET client_min_messages TO ERROR;
\ir :TEST_SUPPORT_FILE
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
\o
SET client_min_messages TO NOTICE;
\set ECHO all

SET ROLE :ROLE_1;

CREATE TABLE hyper (
  time BIGINT NOT NULL,
  device_id TEXT NOT NULL,
  sensor_1 NUMERIC NULL DEFAULT 1
);

-- Table to log trigger events
CREATE TABLE trigger_events (
   tg_when text,
   tg_level text,
   tg_op text,
   tg_name text
);
  
CREATE OR REPLACE FUNCTION test_trigger()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    INSERT INTO public.trigger_events VALUES (TG_WHEN, TG_LEVEL, TG_OP, TG_NAME);	
    RETURN NEW;
END
$BODY$;

CALL distributed_exec($$
CREATE TABLE trigger_events (
   tg_when text,
   tg_level text,
   tg_op text,
   tg_name text
);
$$, ARRAY[:'DATA_NODE_1']);

-- row triggers: BEFORE
CREATE TRIGGER _0_test_trigger_insert
    BEFORE INSERT ON hyper
    FOR EACH ROW EXECUTE FUNCTION test_trigger();

CREATE TRIGGER _0_test_trigger_update
    BEFORE UPDATE ON hyper
    FOR EACH ROW EXECUTE FUNCTION test_trigger();

CREATE TRIGGER _0_test_trigger_delete
    BEFORE DELETE ON hyper
    FOR EACH ROW EXECUTE FUNCTION test_trigger();

CREATE TRIGGER z_test_trigger_all
    BEFORE INSERT OR UPDATE OR DELETE ON hyper
    FOR EACH ROW EXECUTE FUNCTION test_trigger();

-- row triggers: AFTER
CREATE TRIGGER _0_test_trigger_insert_after
    AFTER INSERT ON hyper
    FOR EACH ROW EXECUTE FUNCTION test_trigger();

CREATE TRIGGER _0_test_trigger_insert_after_when_dev1
    AFTER INSERT ON hyper
    FOR EACH ROW
    WHEN (NEW.device_id = 'dev1')
    EXECUTE FUNCTION test_trigger();

CREATE TRIGGER _0_test_trigger_update_after
    AFTER UPDATE ON hyper
    FOR EACH ROW EXECUTE FUNCTION test_trigger();

CREATE TRIGGER _0_test_trigger_delete_after
    AFTER DELETE ON hyper
    FOR EACH ROW EXECUTE FUNCTION test_trigger();

CREATE TRIGGER z_test_trigger_all_after
    AFTER INSERT OR UPDATE OR DELETE ON hyper
    FOR EACH ROW EXECUTE FUNCTION test_trigger();


--- Create some triggers before we turn the table into a distributed
--- hypertable and some triggers after so that we test both cases.
SELECT * FROM create_distributed_hypertable('hyper', 'time', 'device_id', 3, chunk_time_interval => 10, data_nodes => ARRAY[:'DATA_NODE_1', :'DATA_NODE_2']);

-- FAILURE cases
\set ON_ERROR_STOP 0

-- Check that CREATE TRIGGER fails if a trigger already exists on a data node.
CALL distributed_exec($$
CREATE TRIGGER _0_test_trigger_insert_s_before
    BEFORE INSERT ON hyper
    FOR EACH STATEMENT EXECUTE FUNCTION test_trigger();
$$, ARRAY[:'DATA_NODE_1']);

CREATE TRIGGER _0_test_trigger_insert_s_before
    BEFORE INSERT ON hyper
    FOR EACH STATEMENT EXECUTE FUNCTION test_trigger();

CALL distributed_exec($$
DROP TRIGGER _0_test_trigger_insert_s_before ON hyper;
$$, ARRAY[:'DATA_NODE_1']);

-- Test that trigger execution fails if trigger_events table doesn't
-- exist on all nodes.  Insert should fail
INSERT INTO hyper(time, device_id,sensor_1) VALUES
(1257987600000000000, 'dev1', 1);
\set ON_ERROR_STOP 1

-- Now, create trigger_events on the other nodes
CALL distributed_exec($$
CREATE TABLE trigger_events (
   tg_when text,
   tg_level text,
   tg_op text,
   tg_name text
);
$$, ARRAY[:'DATA_NODE_2', :'DATA_NODE_3']);

-- Test that trigger fails if the user isn't the owner of the trigger
-- function on one of the nodes.
RESET ROLE;
CALL distributed_exec($$
     ALTER FUNCTION test_trigger OWNER TO current_user;
$$, ARRAY[:'DATA_NODE_1']);
SET ROLE :ROLE_1;

\set ON_ERROR_STOP 0
-- Insert should fail since the trigger function on DN1 isn't owned by
-- the user.
INSERT INTO hyper(time, device_id,sensor_1) VALUES
(1257987600000000000, 'dev1', 1);
\set ON_ERROR_STOP 1

-- Reset the owner of the trigger function on DN1 to the non-superuser
RESET ROLE;
CALL distributed_exec('ALTER FUNCTION test_trigger OWNER TO ' || :'ROLE_1', ARRAY[:'DATA_NODE_1']);
SET ROLE :ROLE_1;

-- Add more triggers after the distributed hypertable is created

-- statement triggers: BEFORE
CREATE TRIGGER _0_test_trigger_insert_s_before
    BEFORE INSERT ON hyper
    FOR EACH STATEMENT EXECUTE FUNCTION test_trigger();

CREATE TRIGGER _0_test_trigger_update_s_before
    BEFORE UPDATE ON hyper
    FOR EACH STATEMENT EXECUTE FUNCTION test_trigger();

CREATE TRIGGER _0_test_trigger_delete_s_before
    BEFORE DELETE ON hyper
    FOR EACH STATEMENT EXECUTE FUNCTION test_trigger();

-- statement triggers: AFTER
CREATE TRIGGER _0_test_trigger_insert_s_after
    AFTER INSERT ON hyper
    FOR EACH STATEMENT EXECUTE FUNCTION test_trigger();

CREATE TRIGGER _0_test_trigger_update_s_after
    AFTER UPDATE ON hyper
    FOR EACH STATEMENT EXECUTE FUNCTION test_trigger();

CREATE TRIGGER _0_test_trigger_delete_s_after
    AFTER DELETE ON hyper
    FOR EACH STATEMENT EXECUTE FUNCTION test_trigger();

--test triggers before create_distributed_hypertable
INSERT INTO hyper(time, device_id,sensor_1) VALUES
(1257987600000000000, 'dev1', 1);

-- Show trigger count on access node. Only statement-level triggers
-- fire on the access node.
SELECT tg_when, tg_level, tg_op, tg_name, count(*)
FROM trigger_events
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;

-- Show trigger counts on data nodes. Both statement-level and
-- row-level triggers fire on the data nodes.
SELECT * FROM test.remote_exec(ARRAY[:'DATA_NODE_1', :'DATA_NODE_2'], $$
SELECT tg_when, tg_level, tg_op, tg_name, count(*)
FROM trigger_events
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;
$$);

TRUNCATE trigger_events;
CALL distributed_exec($$
TRUNCATE trigger_events;
$$);

INSERT INTO hyper(time, device_id,sensor_1) VALUES
(1257987700000000000, 'dev2', 1), (1257987800000000000, 'dev2', 1);

UPDATE hyper SET sensor_1 = 2;

DELETE FROM hyper;

SELECT tg_when, tg_level, tg_op, tg_name, count(*)
FROM trigger_events
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;

SELECT * FROM test.remote_exec(ARRAY[:'DATA_NODE_1', :'DATA_NODE_2'], $$
SELECT tg_when, tg_level, tg_op, tg_name, count(*)
FROM trigger_events
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;
$$);

-- Attach a new data node and show that the hypertable is created on
-- the node, including its triggers.
SELECT attach_data_node(:'DATA_NODE_3', 'hyper');

-- Show that triggers are created on the new data node after attaching
SELECT * FROM test.remote_exec(ARRAY[:'DATA_NODE_3'],
$$
    SELECT test.show_triggers('hyper');
$$);

-- Insert data on the new data node to create a chunk and fire
-- triggers.
INSERT INTO hyper(time, device_id,sensor_1) VALUES
(1257987700000000000, 'dev4', 1);

SELECT * FROM test.remote_exec(ARRAY[:'DATA_NODE_3'], $$
SELECT tg_when, tg_level, tg_op, tg_name, count(*)
FROM trigger_events
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;
$$);

--test drop trigger
DROP TRIGGER _0_test_trigger_insert ON hyper;
DROP TRIGGER _0_test_trigger_insert_s_before ON hyper;
DROP TRIGGER _0_test_trigger_insert_after ON hyper;
DROP TRIGGER _0_test_trigger_insert_s_after ON hyper;

DROP TRIGGER _0_test_trigger_update ON hyper;
DROP TRIGGER _0_test_trigger_update_s_before ON hyper;
DROP TRIGGER _0_test_trigger_update_after ON hyper;
DROP TRIGGER _0_test_trigger_update_s_after ON hyper;

DROP TRIGGER _0_test_trigger_delete ON hyper;
DROP TRIGGER _0_test_trigger_delete_s_before ON hyper;
DROP TRIGGER _0_test_trigger_delete_after ON hyper;
DROP TRIGGER _0_test_trigger_delete_s_after ON hyper;

DROP TRIGGER z_test_trigger_all ON hyper;
DROP TRIGGER z_test_trigger_all_after ON hyper;
DROP TRIGGER _0_test_trigger_insert_after_when_dev1 ON hyper;

-- Triggers are dropped on all data nodes:
SELECT * FROM test.remote_exec(ARRAY[:'DATA_NODE_1', :'DATA_NODE_2', :'DATA_NODE_3'], $$
SELECT st."Child" as chunk_relid, test.show_triggers((st)."Child")
FROM test.show_subtables('hyper') st;
$$);

-- Test triggers that modify tuples and make sure RETURNING is done
-- properly (i.e., the modified tuple is returned).

-- Add serial (autoincrement) and DEFAULT value columns to test that
-- these work with custom insert nodes.
CREATE TABLE disttable(
    id serial,
    time timestamptz NOT NULL,
    device int DEFAULT 100,
    temp_c float
);
SELECT * FROM create_distributed_hypertable('disttable', 'time', 'device');

-- Create a datatable to source data from. Add array of composite data
-- type to test switching to text mode below. Arrays include the type
-- Oid when serialized in binary format. Since the Oid of a
-- user-created type can differ across data nodes, such serialization
-- is not safe.
CREATE TABLE datatable (LIKE disttable);
INSERT INTO datatable (id, time, device, temp_c) VALUES
       (1, '2017-01-01 06:01', 1, 1),
       (2, '2017-01-01 09:11', 3, 2),
       (3, '2017-01-01 08:01', 1, 3),
       (4, '2017-01-02 08:01', 2, 4),
       (5, '2018-07-02 08:01', 87, 5),
       (6, '2018-07-01 06:01', 13, 6),
       (7, '2018-07-01 09:11', 90, 7),
       (8, '2018-07-01 08:01', 29, 8);

CREATE OR REPLACE FUNCTION temp_increment_trigger()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    IF TG_OP = 'INSERT' THEN
	   NEW.temp_c = NEW.temp_c+1.0;	   
	END IF;
    RETURN NEW;
END
$BODY$;

-- Add a BEFORE INSERT trigger to see that plan reverts to
-- DataNodeDispatch when using RETURNING
CREATE TRIGGER _0_temp_increment
    BEFORE INSERT ON disttable
    FOR EACH ROW EXECUTE FUNCTION temp_increment_trigger();

-- Show that the trigger exists on a data node
SELECT test.remote_exec(ARRAY[:'DATA_NODE_3'], $$ SELECT test.show_triggers('disttable') $$);
TRUNCATE disttable;

-- Show EXPLAINs for INSERT first with DataNodeCopy disabled. Should
-- always use DataNodeDispatch
SET timescaledb.enable_distributed_insert_with_copy=false;
-- Without RETURNING
EXPLAIN VERBOSE
INSERT INTO disttable (time, device, temp_c)
SELECT time, device, temp_c FROM datatable;

-- With RETURNING
EXPLAIN VERBOSE
INSERT INTO disttable (time, device, temp_c)
SELECT time, device, temp_c FROM datatable RETURNING *;

-- With DataNodeCopy enabled, should use DataNodeCopy when there's no
-- RETURNING clause, but with RETURNING it should use DataNodeDispatch
-- due to the modifying trigger.
SET timescaledb.enable_distributed_insert_with_copy=true;
-- Without RETURNING
EXPLAIN VERBOSE
INSERT INTO disttable (time, device, temp_c)
SELECT time, device, temp_c FROM datatable;

-- With RETURNING
EXPLAIN VERBOSE
INSERT INTO disttable (time, device, temp_c)
SELECT time, device, temp_c FROM datatable RETURNING *;

-- Do the actual INSERT, but wrap in CTE to ensure ordered output in
-- order to avoid flakiness. The returned rows should have temp_c
-- incremented by the trigger
WITH inserted AS (
	 INSERT INTO disttable (time, device, temp_c)
	 SELECT time, device, temp_c FROM datatable RETURNING *
) SELECT * FROM inserted ORDER BY 1;

-- Show that the RETURNING rows are the same as those stored after
-- INSERT. Expect temp_c to be incremented by one compared to the
-- original data.
SELECT di.id, di.time, di.device, di.temp_c AS temp_c, da.temp_c AS temp_c_orig
FROM disttable di, datatable da
WHERE di.id = da.id
ORDER BY 1;

-- Rename a trigger
ALTER TRIGGER _0_temp_increment ON disttable RENAME TO _1_temp_increment;

-- Show that remote chunks have the new trigger name
SELECT * FROM test.remote_exec(NULL, $$
SELECT st."Child" as chunk_relid, test.show_triggers((st)."Child")
FROM test.show_subtables('disttable') st;
$$);

-- Drop the trigger and show that it is dropped on data nodes
DROP TRIGGER _1_temp_increment ON disttable;

SELECT * FROM test.remote_exec(NULL, $$
SELECT st."Child" as chunk_relid, test.show_triggers((st)."Child")
FROM test.show_subtables('disttable') st;
$$);
