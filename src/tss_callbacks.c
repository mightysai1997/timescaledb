/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>

#include <fmgr.h>
#include <executor/instrument.h>

#include "guc.h"
#include "tss_callbacks.h"

#define TSS_CALLBACKS_VAR_NAME "tss_callbacks"
#define TSS_CALLBACKS_VERSION 1

typedef void (*tss_store_hook_type)(const char *query, int query_location, int query_len,
									uint64 query_id, uint64 total_time, uint64 rows,
									const BufferUsage *bufusage, const WalUsage *walusage);
typedef bool (*tss_enabled)(int level);

/* ts_stat_statements callbacks */
typedef struct TSSCallbacks
{
	int64 version_num;
	tss_store_hook_type tss_store_hook;
	tss_enabled tss_enabled;
} TSSCallbacks;

static bool started = false;
static instr_time start_time;
static BufferUsage start_bufusage;
static WalUsage start_walusage;

static TSSCallbacks *
ts_get_tss_callbacks(void)
{
	TSSCallbacks **ptr = (TSSCallbacks **) find_rendezvous_variable(TSS_CALLBACKS_VAR_NAME);

	return *ptr;
}

static tss_store_hook_type
ts_get_tss_store_hook()
{
	TSSCallbacks *ptr = ts_get_tss_callbacks();
	if (ptr && ptr->version_num == TSS_CALLBACKS_VERSION)
	{
		return ptr->tss_store_hook;
	}

	return NULL;
}

static bool
is_tss_enabled(void)
{
	if (ts_guc_enable_tss_callbacks)
	{
		TSSCallbacks *ptr = ts_get_tss_callbacks();
		if (ptr && ptr->version_num == TSS_CALLBACKS_VERSION)
		{
			return ptr->tss_enabled(0); /* consider top level statement */
		}
	}

	return false;
}

void
ts_begin_tss_store_callback(void)
{
	if (!is_tss_enabled())
		return;

	start_bufusage = pgBufferUsage;
	start_walusage = pgWalUsage;
	INSTR_TIME_SET_CURRENT(start_time);
	started = true;
}

void
ts_end_tss_store_callback(const char *query, int query_location, int query_len, uint64 query_id,
						  uint64 rows)
{
	if (!started)
		return;

	instr_time duration;
	BufferUsage bufusage;
	WalUsage walusage;

	INSTR_TIME_SET_CURRENT(duration);
	INSTR_TIME_SUBTRACT(duration, start_time);

	/* calc differences of buffer counters. */
	memset(&bufusage, 0, sizeof(BufferUsage));
	BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &start_bufusage);

	/* calc differences of WAL counters. */
	memset(&walusage, 0, sizeof(WalUsage));
	WalUsageAccumDiff(&walusage, &pgWalUsage, &start_walusage);

	tss_store_hook_type hook = ts_get_tss_store_hook();

	if (hook)
		hook(query,
			 query_location,
			 query_len,
			 query_id,
			 INSTR_TIME_GET_MICROSEC(duration),
			 rows,
			 &bufusage,
			 &walusage);

	started = false;
}
