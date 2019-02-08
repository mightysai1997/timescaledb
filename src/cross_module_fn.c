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
TS_FUNCTION_INFO_V1(ts_remove_drop_chunks_policy);
TS_FUNCTION_INFO_V1(ts_remove_reorder_policy);
TS_FUNCTION_INFO_V1(ts_alter_job_schedule);
TS_FUNCTION_INFO_V1(ts_reorder_chunk);

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
ts_alter_job_schedule(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->alter_job_schedule(fcinfo));
}

Datum
ts_reorder_chunk(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->reorder_chunk(fcinfo));
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
add_telemetry_default(JsonbParseState *parseState)
{
	error_no_default_fn_community();
}

static bool
bgw_policy_job_execute_default_fn(BgwJob *job)
{
	error_no_default_fn_enterprise();
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
	.add_tsl_license_info_telemetry = add_telemetry_default,
	.bgw_policy_job_execute = bgw_policy_job_execute_default_fn,
	.add_drop_chunks_policy = error_no_default_fn_pg_enterprise,
	.add_reorder_policy = error_no_default_fn_pg_enterprise,
	.remove_drop_chunks_policy = error_no_default_fn_pg_enterprise,
	.remove_reorder_policy = error_no_default_fn_pg_enterprise,
	.create_upper_paths_hook = NULL,
	.gapfill_marker = error_no_default_fn_pg_community,
	.gapfill_int16_time_bucket = error_no_default_fn_pg_community,
	.gapfill_int32_time_bucket = error_no_default_fn_pg_community,
	.gapfill_int64_time_bucket = error_no_default_fn_pg_community,
	.gapfill_date_time_bucket = error_no_default_fn_pg_community,
	.gapfill_timestamp_time_bucket = error_no_default_fn_pg_community,
	.gapfill_timestamptz_time_bucket = error_no_default_fn_pg_community,
	.alter_job_schedule = error_no_default_fn_pg_enterprise,
	.reorder_chunk = error_no_default_fn_pg_community,
};

TSDLLEXPORT CrossModuleFunctions *ts_cm_functions = &ts_cm_functions_default;
