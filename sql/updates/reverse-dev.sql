DROP VIEW _timescaledb_internal.hypertable_chunk_local_size;
DROP FUNCTION _timescaledb_internal.relation_size(relation REGCLASS);
DROP INDEX _timescaledb_catalog.chunk_constraint_dimension_slice_id_idx;
CREATE INDEX chunk_constraint_chunk_id_dimension_slice_id_idx ON _timescaledb_catalog.chunk_constraint (chunk_id, dimension_slice_id);

DROP FUNCTION IF EXISTS @extschema@.add_policies;
DROP FUNCTION IF EXISTS @extschema@.remove_policies;
DROP FUNCTION IF EXISTS @extschema@.alter_policies;
DROP FUNCTION IF EXISTS @extschema@.show_policies;
