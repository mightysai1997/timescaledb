/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

extern void ts_begin_tss_store_callback(void);
extern void ts_end_tss_store_callback(const char *query, int query_location, int query_len,
									  uint64 query_id, uint64 rows);
