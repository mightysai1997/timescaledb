/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include "hypertable.h"

/* HypertableRestrictInfo represents restrictions on a hypertable. It uses
 * range exclusion logic to figure out which chunks can match the description */
typedef struct HypertableRestrictInfo HypertableRestrictInfo;

extern HypertableRestrictInfo *ts_hypertable_restrict_info_create(RelOptInfo *rel, Hypertable *ht);

/* Add restrictions based on a List of RestrictInfo */
extern void ts_hypertable_restrict_info_add(HypertableRestrictInfo *hri, PlannerInfo *root,
											List *base_restrict_infos);

/* Get a list of chunk oids for chunks whose constraints match the restriction clauses */
extern Chunk **ts_hypertable_restrict_info_get_chunks(HypertableRestrictInfo *hri, Hypertable *ht,
													  unsigned int *num_chunks);

extern Chunk **ts_hypertable_restrict_info_get_chunks_ordered(HypertableRestrictInfo *hri,
															  Hypertable *ht, Chunk **chunks,
															  bool reverse, List **nested_oids,
															  unsigned int *num_chunks);
