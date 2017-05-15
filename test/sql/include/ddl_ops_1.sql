CREATE TABLE PUBLIC."Hypertable_1" (
  time BIGINT NOT NULL,
  "Device_id" TEXT NOT NULL,
  temp_c int NOT NULL DEFAULT -1,
  humidity numeric NULL DEFAULT 0,
  sensor_1 NUMERIC NULL DEFAULT 1,
  sensor_2 NUMERIC NOT NULL DEFAULT 1,
  sensor_3 NUMERIC NOT NULL DEFAULT 1,
  sensor_4 NUMERIC NOT NULL DEFAULT 1
);
CREATE INDEX ON PUBLIC."Hypertable_1" (time, "Device_id");

\set ON_ERROR_STOP 0
CREATE SCHEMA IF NOT EXISTS "customSchema";
\set ON_ERROR_STOP 1

CREATE TABLE "customSchema"."Hypertable_1" (
  time BIGINT NOT NULL,
  "Device_id" TEXT NOT NULL,
  temp_c int NOT NULL DEFAULT -1,
  humidity numeric NULL DEFAULT 0,
  sensor_1 NUMERIC NULL DEFAULT 1,
  sensor_2 NUMERIC NOT NULL DEFAULT 1,
  sensor_3 NUMERIC NOT NULL DEFAULT 1,
  sensor_4 NUMERIC NOT NULL DEFAULT 1
);
CREATE INDEX ON "customSchema"."Hypertable_1" (time, "Device_id");

SELECT * FROM create_hypertable('"public"."Hypertable_1"', 'time', 'Device_id', 1);

SELECT * FROM create_hypertable('"customSchema"."Hypertable_1"', 'time', NULL, 1);

SELECT * FROM _timescaledb_catalog.hypertable;
SELECT * FROM _timescaledb_catalog.hypertable_index;

CREATE INDEX ON PUBLIC."Hypertable_1" (time, "temp_c");
CREATE INDEX "ind_humidity" ON PUBLIC."Hypertable_1" (time, "humidity");
CREATE INDEX "ind_sensor_1" ON PUBLIC."Hypertable_1" (time, "sensor_1");

INSERT INTO PUBLIC."Hypertable_1"(time, "Device_id", temp_c, humidity, sensor_1, sensor_2, sensor_3, sensor_4)
VALUES(1257894000000000000, 'dev1', 30, 70, 1, 2, 3, 100);


CREATE UNIQUE INDEX "Unique1" ON PUBLIC."Hypertable_1" (time, "Device_id");
CREATE UNIQUE INDEX "Unique1" ON "customSchema"."Hypertable_1" (time);

INSERT INTO "customSchema"."Hypertable_1"(time, "Device_id", temp_c, humidity, sensor_1, sensor_2, sensor_3, sensor_4)
VALUES(1257894000000000000, 'dev1', 30, 70, 1, 2, 3, 100);
INSERT INTO "customSchema"."Hypertable_1"(time, "Device_id", temp_c, humidity, sensor_1, sensor_2, sensor_3, sensor_4)
VALUES(1257894000000000001, 'dev1', 30, 70, 1, 2, 3, 100);

SELECT * FROM _timescaledb_catalog.hypertable_index;

--expect error cases
\set ON_ERROR_STOP 0
INSERT INTO "customSchema"."Hypertable_1"(time, "Device_id", temp_c, humidity, sensor_1, sensor_2, sensor_3, sensor_4)
VALUES(1257894000000000000, 'dev1', 31, 71, 72, 4, 1, 102);
CREATE UNIQUE INDEX "Unique2" ON PUBLIC."Hypertable_1" ("Device_id");
CREATE UNIQUE INDEX "Unique2" ON PUBLIC."Hypertable_1" (time);
CREATE UNIQUE INDEX "Unique2" ON PUBLIC."Hypertable_1" (sensor_1);
UPDATE ONLY PUBLIC."Hypertable_1" SET time = 0 WHERE TRUE;
DELETE FROM ONLY PUBLIC."Hypertable_1" WHERE "Device_id" = 'dev1';
\set ON_ERROR_STOP 1


CREATE TABLE my_ht (time bigint, val integer);
SELECT * FROM create_hypertable('my_ht', 'time');
ALTER TABLE my_ht ADD COLUMN val2 integer;
\d my_ht

-- Should error when adding again
\set ON_ERROR_STOP 0
ALTER TABLE my_ht ADD COLUMN val2 integer;
\set ON_ERROR_STOP 1

-- Should create 
ALTER TABLE my_ht ADD COLUMN IF NOT EXISTS val3 integer;
\d my_ht

-- Should skip and not error
ALTER TABLE my_ht ADD COLUMN IF NOT EXISTS val3 integer;
\d my_ht

-- Should drop
ALTER TABLE my_ht DROP COLUMN IF EXISTS val3;
\d my_ht

-- Should skip and not error
ALTER TABLE my_ht DROP COLUMN IF EXISTS val3;
\d my_ht
