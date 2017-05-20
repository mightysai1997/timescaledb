-- This file defines DDL functions for adding and manipulating hypertables.

-- Converts a regular postgres table to a hypertable.
--
-- main_table - The OID of the table to be converted
-- time_column_name - Name of the column that contains time for a given record
-- partitioning_column - Name of the column to partition data by
-- replication_factor -- (Optional) Number of replicas for data
-- number_partitions - (Optional) Number of partitions for data
-- associated_schema_name - (Optional) Schema for internal hypertable tables
-- associated_table_prefix - (Optional) Prefix for internal hypertable table names
CREATE OR REPLACE FUNCTION  create_hypertable(
    main_table              REGCLASS,
    time_column_name        NAME,
    partitioning_column     NAME = NULL,
    number_partitions       INTEGER = NULL,
    replication_factor      SMALLINT = 1,
    associated_schema_name  NAME = NULL,
    associated_table_prefix NAME = NULL,
    placement               _timescaledb_catalog.chunk_placement_type = 'STICKY',
    chunk_time_interval     BIGINT =  _timescaledb_internal.interval_to_usec('1 month'),
    create_default_indexes  BOOLEAN = TRUE
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    hypertable_row   _timescaledb_catalog.hypertable;
    table_name       NAME;
    schema_name      NAME;
    tablespace_oid   OID;
    tablespace_name  NAME;
    time_column_type REGTYPE;
    att_row          pg_attribute;
    main_table_has_items BOOLEAN;
BEGIN
    SELECT relname, nspname, reltablespace
    INTO STRICT table_name, schema_name, tablespace_oid
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = main_table;

    --tables that don't have an associated tablespace has reltablespace OID set to 0
    --in pg_class and there is no matching row in pg_tablespace
    SELECT spcname
    INTO tablespace_name
    FROM pg_tablespace t
    WHERE t.OID = tablespace_oid;

    BEGIN
        SELECT atttypid
        INTO STRICT time_column_type
        FROM pg_attribute
        WHERE attrelid = main_table AND attname = time_column_name;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE EXCEPTION 'column "%" does not exist', time_column_name
            USING ERRCODE = 'IO102';
    END;

    IF time_column_type NOT IN ('BIGINT', 'INTEGER', 'SMALLINT', 'TIMESTAMP', 'TIMESTAMPTZ') THEN
        RAISE EXCEPTION 'illegal type for time column "%": %', time_column_name, time_column_type
        USING ERRCODE = 'IO102';
    END IF;

    IF partitioning_column IS NOT NULL THEN
        PERFORM atttypid
        FROM pg_attribute
        WHERE attrelid = main_table AND attname = partitioning_column;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'column "%" does not exist', partitioning_column
            USING ERRCODE = 'IO102';
        END IF;
    END IF;

    EXECUTE format('SELECT TRUE FROM %s LIMIT 1', main_table) INTO main_table_has_items;

    IF main_table_has_items THEN
        RAISE EXCEPTION 'the table being converted to a hypertable must be empty'
        USING ERRCODE = 'IO102';
    END IF;

    BEGIN
        SELECT *
        INTO hypertable_row
        FROM  _timescaledb_meta_api.create_hypertable(
            schema_name,
            table_name,
            time_column_name,
            time_column_type,
            partitioning_column,
            number_partitions,
            replication_factor,
            associated_schema_name,
            associated_table_prefix,
            placement,
            chunk_time_interval,
            tablespace_name
        );
    EXCEPTION
        WHEN unique_violation THEN
            RAISE EXCEPTION 'hypertable % already exists', main_table
            USING ERRCODE = 'IO110';
        WHEN foreign_key_violation THEN
            RAISE EXCEPTION 'database not configured for hypertable storage (not setup as a data-node)'
            USING ERRCODE = 'IO101';
    END;

    FOR att_row IN SELECT *
    FROM pg_attribute att
    WHERE attrelid = main_table AND attnum > 0 AND NOT attisdropped
    LOOP
        PERFORM  _timescaledb_internal.create_column_from_attribute(hypertable_row.id, att_row);
    END LOOP;

    PERFORM 1
    FROM pg_index,
    LATERAL _timescaledb_meta_api.add_index(
        hypertable_row.id,
        hypertable_row.schema_name,
        (SELECT relname FROM pg_class WHERE oid = indexrelid::regclass),
        _timescaledb_internal.get_general_index_definition(indexrelid, indrelid, hypertable_row)
    )
    WHERE indrelid = main_table;

    IF create_default_indexes THEN
        PERFORM _timescaledb_internal.create_default_indexes(hypertable_row, main_table, partitioning_column);
    END IF;
END
$BODY$;

-- Update chunk_time_interval for hypertable
CREATE OR REPLACE FUNCTION  set_chunk_time_interval(
    main_table              REGCLASS,
    chunk_time_interval     BIGINT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    table_name       NAME;
    schema_name      NAME;
    tablespace_oid   OID;
    tablespace_name  NAME;
BEGIN
    SELECT relname, nspname, reltablespace
    INTO STRICT table_name, schema_name, tablespace_oid
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = main_table;

    PERFORM _timescaledb_meta_api.set_chunk_time_interval(
                                        schema_name,
                                        table_name,
                                        chunk_time_interval);
END
$BODY$;
