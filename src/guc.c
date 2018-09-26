#include <postgres.h>
#include <utils/guc.h>
#include <miscadmin.h>

#include "guc.h"
#include "hypertable_cache.h"
#include "telemetry/telemetry.h"

typedef enum TelemetryLevel
{
	TELEMETRY_OFF,
	TELEMETRY_BASIC,
} TelemetryLevel;

/* Define which level means on. We use this object to have at least one object
 * of type TelemetryLevel in the code, otherwise pgindent won't work for the
 * type */
static const TelemetryLevel on_level = TELEMETRY_BASIC;

bool
telemetry_on()
{
	return guc_telemetry_level == on_level;
}

static const struct config_enum_entry telemetry_level_options[] =
{
	{"off", TELEMETRY_OFF, false},
	{"basic", TELEMETRY_BASIC, false},
	{NULL, 0, false}
};

bool		guc_disable_optimizations = false;
bool		guc_optimize_non_hypertables = false;
bool		guc_restoring = false;
bool		guc_constraint_aware_append = true;
int			guc_max_open_chunks_per_insert = 10;
int			guc_max_cached_chunks_per_hypertable = 10;
int			guc_telemetry_level = TELEMETRY_BASIC;

static void
assign_max_cached_chunks_per_hypertable_hook(int newval, void *extra)
{
	/* invalidate the hypertable cache to reset */
	hypertable_cache_invalidate_callback();
}

void
_guc_init(void)
{
	/* Main database to connect to. */
	DefineCustomBoolVariable("timescaledb.disable_optimizations", "Disable all timescale query optimizations",
							 NULL,
							 &guc_disable_optimizations,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
	DefineCustomBoolVariable("timescaledb.optimize_non_hypertables", "Apply timescale query optimization to plain tables",
							 "Apply timescale query optimization to plain tables in addition to hypertables",
							 &guc_optimize_non_hypertables,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("timescaledb.restoring", "Install timescale in restoring mode",
							 "Used for running pg_restore",
							 &guc_restoring,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("timescaledb.constraint_aware_append", "Enable constraint-aware append scans",
							 "Enable constraint exclusion at execution time",
							 &guc_constraint_aware_append,
							 true,
							 PGC_USERSET
							 ,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("timescaledb.max_open_chunks_per_insert",
							"Maximum open chunks per insert",
							"Maximum number of open chunk tables per insert",
							&guc_max_open_chunks_per_insert,
							work_mem * 1024L / 25000L,	/* Measurements via
														 * `MemoryContextStats(TopMemoryContext)`
														 * show chunk insert
														 * state memory context
														 * takes up ~25K bytes
														 * (work_mem is in
														 * kbytes) */
							0,
							65536,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("timescaledb.max_cached_chunks_per_hypertable",
							"Maximum cached chunks",
							"Maximum number of chunks stored in the cache",
							&guc_max_cached_chunks_per_hypertable,
							100,
							0,
							65536,
							PGC_USERSET,
							0,
							NULL,
							assign_max_cached_chunks_per_hypertable_hook,
							NULL);
	DefineCustomEnumVariable("timescaledb.telemetry_level",
							 "Telemetry settings level",
							 "Level used to determine which telemetry to send",
							 &guc_telemetry_level,
							 TELEMETRY_BASIC,
							 telemetry_level_options,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
}

void
_guc_fini(void)
{
}
