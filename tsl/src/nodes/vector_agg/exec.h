/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#pragma once

#include <postgres.h>

#include <nodes/execnodes.h>

#include "functions.h"

typedef struct
{
	VectorAggFunctions *func;
	int input_offset;
	int output_offset;
} VectorAggDef;

typedef struct
{
	CustomScanState custom;

	List *agg_defs;

	/*
	 * We can't call the underlying scan after it has ended, or it will be
	 * restarted. This is the behavior of Postgres heap scans. So we have to
	 * track whether it has ended to avoid this.
	 */
	bool input_ended;

	/* The following fields are related to the grouping policy. */
	List *agg_states;
} VectorAggState;

extern Node *vector_agg_state_create(CustomScan *cscan);