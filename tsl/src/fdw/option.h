/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

extern void option_validate(List *options_list, Oid catalog);
extern List *option_extract_extension_list(const char *extensions_string, bool warn_on_missing);
extern List *option_extract_join_ref_table_list(const char *join_tables);
extern bool option_get_from_options_list_int(List *options, const char *optionname, int *value);
