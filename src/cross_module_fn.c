/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>
#include <utils/timestamp.h>
#include <utils/lsyscache.h>

#include "export.h"
#include "cross_module_fn.h"
#include "guc.h"
#include "bgw/job.h"

TS_FUNCTION_INFO_V1(ts_add_drop_chunks_policy);
TS_FUNCTION_INFO_V1(ts_add_reorder_policy);
TS_FUNCTION_INFO_V1(ts_add_compress_chunks_policy);
TS_FUNCTION_INFO_V1(ts_remove_drop_chunks_policy);
TS_FUNCTION_INFO_V1(ts_remove_reorder_policy);
TS_FUNCTION_INFO_V1(ts_remove_compress_chunks_policy);
TS_FUNCTION_INFO_V1(ts_alter_job_schedule);
TS_FUNCTION_INFO_V1(ts_reorder_chunk);
TS_FUNCTION_INFO_V1(ts_move_chunk);
TS_FUNCTION_INFO_V1(ts_partialize_agg);
TS_FUNCTION_INFO_V1(ts_finalize_agg_sfunc);
TS_FUNCTION_INFO_V1(ts_finalize_agg_ffunc);
TS_FUNCTION_INFO_V1(ts_continuous_agg_invalidation_trigger);
TS_FUNCTION_INFO_V1(ts_compress_chunk);
TS_FUNCTION_INFO_V1(ts_decompress_chunk);
TS_FUNCTION_INFO_V1(ts_compressed_data_decompress_forward);
TS_FUNCTION_INFO_V1(ts_compressed_data_decompress_reverse);

TS_FUNCTION_INFO_V1(ts_compressed_data_send);
TS_FUNCTION_INFO_V1(ts_compressed_data_recv);
TS_FUNCTION_INFO_V1(ts_compressed_data_in);
TS_FUNCTION_INFO_V1(ts_compressed_data_out);

TS_FUNCTION_INFO_V1(ts_deltadelta_compressor_append);
TS_FUNCTION_INFO_V1(ts_deltadelta_compressor_finish);
TS_FUNCTION_INFO_V1(ts_gorilla_compressor_append);
TS_FUNCTION_INFO_V1(ts_gorilla_compressor_finish);
TS_FUNCTION_INFO_V1(ts_dictionary_compressor_append);
TS_FUNCTION_INFO_V1(ts_dictionary_compressor_finish);
TS_FUNCTION_INFO_V1(ts_array_compressor_append);
TS_FUNCTION_INFO_V1(ts_array_compressor_finish);

TS_FUNCTION_INFO_V1(ts_data_node_add);
TS_FUNCTION_INFO_V1(ts_data_node_delete);
TS_FUNCTION_INFO_V1(ts_data_node_attach);
TS_FUNCTION_INFO_V1(ts_data_node_ping);
TS_FUNCTION_INFO_V1(ts_data_node_detach);
TS_FUNCTION_INFO_V1(ts_data_node_block_new_chunks);
TS_FUNCTION_INFO_V1(ts_data_node_allow_new_chunks);
TS_FUNCTION_INFO_V1(ts_chunk_set_default_data_node);
TS_FUNCTION_INFO_V1(ts_chunk_get_relstats);
TS_FUNCTION_INFO_V1(ts_chunk_get_colstats);
TS_FUNCTION_INFO_V1(ts_timescaledb_fdw_handler);
TS_FUNCTION_INFO_V1(ts_timescaledb_fdw_validator);
TS_FUNCTION_INFO_V1(ts_remote_txn_id_in);
TS_FUNCTION_INFO_V1(ts_remote_txn_id_out);
TS_FUNCTION_INFO_V1(ts_remote_txn_heal_data_node);
TS_FUNCTION_INFO_V1(ts_dist_set_id);
TS_FUNCTION_INFO_V1(ts_dist_set_peer_id);
TS_FUNCTION_INFO_V1(ts_dist_remote_hypertable_info);
TS_FUNCTION_INFO_V1(ts_dist_validate_as_data_node);
TS_FUNCTION_INFO_V1(ts_distributed_exec);
TS_FUNCTION_INFO_V1(ts_hypertable_distributed_set_replication_factor);

Datum
ts_add_drop_chunks_policy(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->add_drop_chunks_policy(fcinfo));
}

Datum
ts_add_reorder_policy(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->add_reorder_policy(fcinfo));
}

Datum
ts_add_compress_chunks_policy(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->add_compress_chunks_policy(fcinfo));
}

Datum
ts_remove_drop_chunks_policy(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->remove_drop_chunks_policy(fcinfo));
}

Datum
ts_remove_reorder_policy(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->remove_reorder_policy(fcinfo));
}

Datum
ts_remove_compress_chunks_policy(PG_FUNCTION_ARGS)
{
	return ts_cm_functions->remove_compress_chunks_policy(fcinfo);
}

Datum
ts_alter_job_schedule(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->alter_job_schedule(fcinfo));
}

Datum
ts_reorder_chunk(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->reorder_chunk(fcinfo));
}

Datum
ts_move_chunk(PG_FUNCTION_ARGS)
{
	return ts_cm_functions->move_chunk(fcinfo);
}

Datum
ts_continuous_agg_invalidation_trigger(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->continuous_agg_trigfn(fcinfo));
}

Datum
ts_data_node_add(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->add_data_node(fcinfo));
}

Datum
ts_data_node_delete(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->delete_data_node(fcinfo));
}

Datum
ts_data_node_attach(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->attach_data_node(fcinfo));
}

Datum
ts_data_node_ping(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->data_node_ping(fcinfo));
}

Datum
ts_data_node_detach(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->detach_data_node(fcinfo));
}

Datum
ts_data_node_block_new_chunks(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->data_node_block_new_chunks(fcinfo));
}

Datum
ts_data_node_allow_new_chunks(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->data_node_allow_new_chunks(fcinfo));
}

Datum
ts_chunk_set_default_data_node(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->set_chunk_default_data_node(fcinfo));
}

Datum
ts_chunk_get_relstats(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->get_chunk_relstats(fcinfo));
}

Datum
ts_chunk_get_colstats(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->get_chunk_colstats(fcinfo));
}

Datum
ts_timescaledb_fdw_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->timescaledb_fdw_handler(fcinfo));
}

Datum
ts_timescaledb_fdw_validator(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->timescaledb_fdw_validator(fcinfo));
}

Datum
ts_remote_txn_id_in(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->remote_txn_id_in(fcinfo));
}

Datum
ts_remote_txn_id_out(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->remote_txn_id_out(fcinfo));
}

Datum
ts_remote_txn_heal_data_node(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->remote_txn_heal_data_node(fcinfo));
}

Datum
ts_dist_set_id(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(ts_cm_functions->set_distributed_id(PG_GETARG_DATUM(0)));
}

Datum
ts_dist_set_peer_id(PG_FUNCTION_ARGS)
{
	ts_cm_functions->set_distributed_peer_id(PG_GETARG_DATUM(0));
	PG_RETURN_VOID();
}

Datum
ts_dist_remote_hypertable_info(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->remote_hypertable_info(fcinfo));
}

Datum
ts_dist_validate_as_data_node(PG_FUNCTION_ARGS)
{
	ts_cm_functions->validate_as_data_node();
	PG_RETURN_VOID();
}

Datum
ts_distributed_exec(PG_FUNCTION_ARGS)
{
	ts_cm_functions->distributed_exec(fcinfo);
	PG_RETURN_VOID();
}

Datum
ts_hypertable_distributed_set_replication_factor(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->set_replication_factor(fcinfo));
}

/*
 * stub function to trigger aggregate util functions.
 */
Datum
ts_partialize_agg(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->partialize_agg(fcinfo));
}

Datum
ts_finalize_agg_sfunc(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->finalize_agg_sfunc(fcinfo));
}

Datum
ts_finalize_agg_ffunc(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->finalize_agg_ffunc(fcinfo));
}

Datum
ts_compressed_data_decompress_forward(PG_FUNCTION_ARGS)
{
	return ts_cm_functions->compressed_data_decompress_forward(fcinfo);
}

Datum
ts_compressed_data_decompress_reverse(PG_FUNCTION_ARGS)
{
	return ts_cm_functions->compressed_data_decompress_reverse(fcinfo);
}

Datum
ts_compressed_data_send(PG_FUNCTION_ARGS)
{
	return ts_cm_functions->compressed_data_send(fcinfo);
}

Datum
ts_compressed_data_recv(PG_FUNCTION_ARGS)
{
	return ts_cm_functions->compressed_data_recv(fcinfo);
}

Datum
ts_compressed_data_in(PG_FUNCTION_ARGS)
{
	return ts_cm_functions->compressed_data_in(fcinfo);
}

Datum
ts_compressed_data_out(PG_FUNCTION_ARGS)
{
	return ts_cm_functions->compressed_data_out(fcinfo);
}

Datum
ts_deltadelta_compressor_append(PG_FUNCTION_ARGS)
{
	return ts_cm_functions->deltadelta_compressor_append(fcinfo);
}

Datum
ts_deltadelta_compressor_finish(PG_FUNCTION_ARGS)
{
	return ts_cm_functions->deltadelta_compressor_finish(fcinfo);
}

Datum
ts_gorilla_compressor_append(PG_FUNCTION_ARGS)
{
	return ts_cm_functions->gorilla_compressor_append(fcinfo);
}

Datum
ts_gorilla_compressor_finish(PG_FUNCTION_ARGS)
{
	return ts_cm_functions->gorilla_compressor_finish(fcinfo);
}

Datum
ts_dictionary_compressor_append(PG_FUNCTION_ARGS)
{
	return ts_cm_functions->dictionary_compressor_append(fcinfo);
}

Datum
ts_dictionary_compressor_finish(PG_FUNCTION_ARGS)
{
	return ts_cm_functions->dictionary_compressor_finish(fcinfo);
}

Datum
ts_array_compressor_append(PG_FUNCTION_ARGS)
{
	return ts_cm_functions->array_compressor_append(fcinfo);
}

Datum
ts_array_compressor_finish(PG_FUNCTION_ARGS)
{
	return ts_cm_functions->array_compressor_finish(fcinfo);
}

Datum
ts_compress_chunk(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->compress_chunk(fcinfo));
}

Datum
ts_decompress_chunk(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->decompress_chunk(fcinfo));
}

/*
 * casting a function pointer to a pointer of another type is undefined
 * behavior, so we need one of these for every function type we have
 */

static void
error_no_default_fn_community(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("functionality not supported under the current license \"%s\", license",
					ts_guc_license_key),
			 errhint(
				 "Upgrade to a Timescale-licensed binary to access this free community feature")));
}

static void
error_no_default_fn_enterprise(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("functionality not supported under the current license \"%s\", license",
					ts_guc_license_key),
			 errhint("Request a trial license to try this feature for free or contact us for more "
					 "information at https://www.timescale.com/pricing")));
}

static bool
error_no_default_fn_bool_void_community(void)
{
	error_no_default_fn_community();
	pg_unreachable();
}

static bool
error_no_default_fn_bool_void_enterprise(void)
{
	error_no_default_fn_enterprise();
	pg_unreachable();
}

static void
tsl_license_on_assign_default_fn(const char *newval, const void *license)
{
	error_no_default_fn_community();
}

static TimestampTz
license_end_time_default_fn(void)
{
	error_no_default_fn_community();
	pg_unreachable();
}

static void
add_tsl_telemetry_info_default(JsonbParseState **parseState)
{
	error_no_default_fn_community();
}

static bool
bgw_policy_job_execute_default_fn(BgwJob *job)
{
	error_no_default_fn_enterprise();
	pg_unreachable();
}

static bool
cagg_materialize_default_fn(int32 materialization_id, ContinuousAggMatOptions *options)
{
	error_no_default_fn_community();
	pg_unreachable();
}

static bool
process_compress_table_default(AlterTableCmd *cmd, Hypertable *ht,
							   WithClauseResult *with_clause_options)
{
	error_no_default_fn_community();
	pg_unreachable();
}

static Datum
error_no_default_fn_pg_community(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function \"%s\" is not supported under the current license \"%s\"",
					get_func_name(fcinfo->flinfo->fn_oid),
					ts_guc_license_key),
			 errhint(
				 "Upgrade to a Timescale-licensed binary to access this free community feature")));
	pg_unreachable();
}

static void
hypertable_make_distributed_default_fn(Hypertable *ht, List *data_node_names)
{
	error_no_default_fn_community();
}

static List *
get_and_validate_data_node_list_default_fn(ArrayType *nodearr)
{
	error_no_default_fn_community();
	pg_unreachable();

	return NIL;
}

static void
cache_syscache_invalidate_default(Datum arg, int cacheid, uint32 hashvalue)
{
	/* The default is a no-op */
}

static Datum
error_no_default_fn_pg_enterprise(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function \"%s\" is not supported under the current license \"%s\"",
					get_func_name(fcinfo->flinfo->fn_oid),
					ts_guc_license_key),
			 errhint("Request a trial license to try this feature for free or contact us for more "
					 "information at https://www.timescale.com/pricing")));
	pg_unreachable();
}

static bool
process_cagg_viewstmt_default(ViewStmt *stmt, const char *query_string, void *pstmt,
							  WithClauseResult *with_clause_options)
{
	return error_no_default_fn_bool_void_community();
}

static void
continuous_agg_update_options_default(ContinuousAgg *cagg, WithClauseResult *with_clause_options)
{
	error_no_default_fn_community();
	pg_unreachable();
}

static void
continuous_agg_drop_chunks_by_chunk_id_default(int32 raw_hypertable_id, Chunk **chunks,
											   Size num_chunks, Datum older_than_datum,
											   Datum newer_than_datum, Oid older_than_type,
											   Oid newer_than_type, int32 log_level,
											   bool user_supplied_table_name)
{
	error_no_default_fn_community();
}

static Datum
empty_fn(PG_FUNCTION_ARGS)
{
	PG_RETURN_VOID();
}

static void
create_chunk_on_data_nodes_default(Chunk *chunk, Hypertable *ht)
{
	error_no_default_fn_community();
}

static Path *
data_node_dispatch_path_create_default(PlannerInfo *root, ModifyTablePath *mtpath,
									   Index hypertable_rti, int subpath_index)
{
	error_no_default_fn_community();
	pg_unreachable();

	return NULL;
}

static void
distributed_copy_default(const CopyStmt *stmt, uint64 *processed, CopyChunkState *ccstate,
						 List *attnums)
{
	error_no_default_fn_community();
}

static bool
set_distributed_id_default(Datum d)
{
	return error_no_default_fn_bool_void_community();
}

static void
set_distributed_peer_id_default(Datum d)
{
	error_no_default_fn_community();
}

static void
func_call_on_data_nodes_default(FunctionCallInfo finfo, List *data_node_oids)
{
	error_no_default_fn_community();
	pg_unreachable();
}

/*
 * Define cross-module functions' default values:
 * If the submodule isn't activated, using one of the cm functions will throw an
 * exception.
 */
TSDLLEXPORT CrossModuleFunctions ts_cm_functions_default = {
	.tsl_license_on_assign = tsl_license_on_assign_default_fn,
	.enterprise_enabled_internal = error_no_default_fn_bool_void_enterprise,
	.check_tsl_loaded = error_no_default_fn_bool_void_community,
	.license_end_time = license_end_time_default_fn,
	.print_tsl_license_expiration_info_hook = NULL,
	.module_shutdown_hook = NULL,
	.add_tsl_telemetry_info = add_tsl_telemetry_info_default,
	.bgw_policy_job_execute = bgw_policy_job_execute_default_fn,
	.continuous_agg_materialize = cagg_materialize_default_fn,
	.add_drop_chunks_policy = error_no_default_fn_pg_community,
	.add_reorder_policy = error_no_default_fn_pg_community,
	.add_compress_chunks_policy = error_no_default_fn_pg_community,
	.remove_drop_chunks_policy = error_no_default_fn_pg_community,
	.remove_reorder_policy = error_no_default_fn_pg_community,
	.remove_compress_chunks_policy = error_no_default_fn_pg_community,
	.create_upper_paths_hook = NULL,
	.set_rel_pathlist_dml = NULL,
	.set_rel_pathlist_query = NULL,
	.set_rel_pathlist = NULL,
	.gapfill_marker = error_no_default_fn_pg_community,
	.gapfill_int16_time_bucket = error_no_default_fn_pg_community,
	.gapfill_int32_time_bucket = error_no_default_fn_pg_community,
	.gapfill_int64_time_bucket = error_no_default_fn_pg_community,
	.gapfill_date_time_bucket = error_no_default_fn_pg_community,
	.gapfill_timestamp_time_bucket = error_no_default_fn_pg_community,
	.gapfill_timestamptz_time_bucket = error_no_default_fn_pg_community,
	.alter_job_schedule = error_no_default_fn_pg_community,
	.reorder_chunk = error_no_default_fn_pg_community,
	.move_chunk = error_no_default_fn_pg_enterprise,
	.ddl_command_start = NULL,
	.ddl_command_end = NULL,
	.sql_drop = NULL,
	.partialize_agg = error_no_default_fn_pg_community,
	.finalize_agg_sfunc = error_no_default_fn_pg_community,
	.finalize_agg_ffunc = error_no_default_fn_pg_community,
	.process_cagg_viewstmt = process_cagg_viewstmt_default,
	.continuous_agg_drop_chunks_by_chunk_id = continuous_agg_drop_chunks_by_chunk_id_default,
	.continuous_agg_trigfn = error_no_default_fn_pg_community,
	.continuous_agg_update_options = continuous_agg_update_options_default,
	.compressed_data_send = error_no_default_fn_pg_community,
	.compressed_data_recv = error_no_default_fn_pg_community,
	.compressed_data_in = error_no_default_fn_pg_community,
	.compressed_data_out = error_no_default_fn_pg_community,
	.process_compress_table = process_compress_table_default,
	.compress_chunk = error_no_default_fn_pg_community,
	.decompress_chunk = error_no_default_fn_pg_community,
	.compressed_data_decompress_forward = error_no_default_fn_pg_community,
	.compressed_data_decompress_reverse = error_no_default_fn_pg_community,
	.deltadelta_compressor_append = error_no_default_fn_pg_community,
	.deltadelta_compressor_finish = error_no_default_fn_pg_community,
	.gorilla_compressor_append = error_no_default_fn_pg_community,
	.gorilla_compressor_finish = error_no_default_fn_pg_community,
	.dictionary_compressor_append = error_no_default_fn_pg_community,
	.dictionary_compressor_finish = error_no_default_fn_pg_community,
	.array_compressor_append = error_no_default_fn_pg_community,
	.array_compressor_finish = error_no_default_fn_pg_community,
	.add_data_node = error_no_default_fn_pg_community,
	.delete_data_node = error_no_default_fn_pg_community,
	.attach_data_node = error_no_default_fn_pg_community,
	.data_node_ping = error_no_default_fn_pg_community,
	.detach_data_node = error_no_default_fn_pg_community,
	.data_node_allow_new_chunks = error_no_default_fn_pg_community,
	.data_node_block_new_chunks = error_no_default_fn_pg_community,
	.distributed_exec = error_no_default_fn_pg_community,
	.set_chunk_default_data_node = error_no_default_fn_pg_community,
	.show_chunk = error_no_default_fn_pg_community,
	.create_chunk = error_no_default_fn_pg_community,
	.create_chunk_on_data_nodes = create_chunk_on_data_nodes_default,
	.hypertable_make_distributed = hypertable_make_distributed_default_fn,
	.get_and_validate_data_node_list = get_and_validate_data_node_list_default_fn,
	.timescaledb_fdw_handler = error_no_default_fn_pg_community,
	.timescaledb_fdw_validator = empty_fn,
	.cache_syscache_invalidate = cache_syscache_invalidate_default,
	.remote_txn_id_in = error_no_default_fn_pg_community,
	.remote_txn_id_out = error_no_default_fn_pg_community,
	.remote_txn_heal_data_node = error_no_default_fn_pg_community,
	.data_node_dispatch_path_create = data_node_dispatch_path_create_default,
	.distributed_copy = distributed_copy_default,
	.set_distributed_id = set_distributed_id_default,
	.set_distributed_peer_id = set_distributed_peer_id_default,
	.is_frontend_session = error_no_default_fn_bool_void_community,
	.remove_from_distributed_db = error_no_default_fn_bool_void_community,
	.remote_hypertable_info = error_no_default_fn_pg_community,
	.validate_as_data_node = error_no_default_fn_community,
	.func_call_on_data_nodes = func_call_on_data_nodes_default,
	.get_chunk_relstats = error_no_default_fn_pg_community,
	.get_chunk_colstats = error_no_default_fn_pg_community,
	.set_replication_factor = error_no_default_fn_pg_community,
};

TSDLLEXPORT CrossModuleFunctions *ts_cm_functions = &ts_cm_functions_default;
