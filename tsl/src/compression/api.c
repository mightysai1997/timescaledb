/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/* This file contains the implementation for SQL utility functions that
 *  compress and decompress chunks
 */
#include <postgres.h>
#include <access/xact.h>
#include <catalog/dependency.h>
#include <commands/tablecmds.h>
#include <commands/trigger.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <nodes/pg_list.h>
#include <nodes/parsenodes.h>
#include <parser/parse_func.h>
#include <storage/lmgr.h>
#include <trigger.h>
#include <utils/builtins.h>
#include <utils/elog.h>
#include <utils/fmgrprotos.h>
#include <libpq-fe.h>
#include <utils/snapmgr.h>
#include <utils/inval.h>

#include <remote/dist_commands.h>
#include "compat/compat.h"
#include "cache.h"
#include "chunk.h"
#include "debug_point.h"
#include "errors.h"
#include "error_utils.h"
#include "hypercube.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "ts_catalog/continuous_agg.h"
#include "ts_catalog/hypertable_compression.h"
#include "ts_catalog/compression_chunk_size.h"
#include "create.h"
#include "api.h"
#include "compression.h"
#include "compat/compat.h"
#include "recompression.h"
#include "scanner.h"
#include "scan_iterator.h"
#include "utils.h"
#include "guc.h"

typedef struct CompressChunkCxt
{
	Hypertable *srcht;
	Chunk *srcht_chunk;		 /* chunk from srcht */
	Hypertable *compress_ht; /*compressed table for srcht */
} CompressChunkCxt;

static void
compression_chunk_size_catalog_insert(int32 src_chunk_id, const RelationSize *src_size,
									  int32 compress_chunk_id, const RelationSize *compress_size,
									  int64 rowcnt_pre_compression, int64 rowcnt_post_compression)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	TupleDesc desc;
	CatalogSecurityContext sec_ctx;

	Datum values[Natts_compression_chunk_size];
	bool nulls[Natts_compression_chunk_size] = { false };

	rel = table_open(catalog_get_table_id(catalog, COMPRESSION_CHUNK_SIZE), RowExclusiveLock);
	desc = RelationGetDescr(rel);

	memset(values, 0, sizeof(values));

	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_chunk_id)] =
		Int32GetDatum(src_chunk_id);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_chunk_id)] =
		Int32GetDatum(compress_chunk_id);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_heap_size)] =
		Int64GetDatum(src_size->heap_size);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_toast_size)] =
		Int64GetDatum(src_size->toast_size);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_index_size)] =
		Int64GetDatum(src_size->index_size);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_heap_size)] =
		Int64GetDatum(compress_size->heap_size);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_toast_size)] =
		Int64GetDatum(compress_size->toast_size);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_index_size)] =
		Int64GetDatum(compress_size->index_size);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_pre_compression)] =
		Int64GetDatum(rowcnt_pre_compression);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_post_compression)] =
		Int64GetDatum(rowcnt_post_compression);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);
	table_close(rel, RowExclusiveLock);
}

static int
compression_chunk_size_catalog_update_merged(int32 chunk_id, const RelationSize *size,
											 int32 merge_chunk_id, const RelationSize *merge_size,
											 int64 merge_rowcnt_pre_compression,
											 int64 merge_rowcnt_post_compression)
{
	ScanIterator iterator =
		ts_scan_iterator_create(COMPRESSION_CHUNK_SIZE, RowExclusiveLock, CurrentMemoryContext);
	bool updated = false;

	iterator.ctx.index =
		catalog_get_index(ts_catalog_get(), COMPRESSION_CHUNK_SIZE, COMPRESSION_CHUNK_SIZE_PKEY);
	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_compression_chunk_size_pkey_chunk_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(chunk_id));
	ts_scanner_foreach(&iterator)
	{
		Datum values[Natts_compression_chunk_size];
		bool replIsnull[Natts_compression_chunk_size] = { false };
		bool repl[Natts_compression_chunk_size] = { false };
		bool should_free;
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
		HeapTuple new_tuple;
		heap_deform_tuple(tuple, ts_scanner_get_tupledesc(ti), values, replIsnull);

		/* Increment existing sizes with sizes from uncompressed chunk. */
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_heap_size)] =
			Int64GetDatum(size->heap_size +
						  DatumGetInt64(values[AttrNumberGetAttrOffset(
							  Anum_compression_chunk_size_uncompressed_heap_size)]));
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_heap_size)] = true;
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_toast_size)] =
			Int64GetDatum(size->toast_size +
						  DatumGetInt64(values[AttrNumberGetAttrOffset(
							  Anum_compression_chunk_size_uncompressed_toast_size)]));
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_toast_size)] = true;
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_index_size)] =
			Int64GetDatum(size->index_size +
						  DatumGetInt64(values[AttrNumberGetAttrOffset(
							  Anum_compression_chunk_size_uncompressed_index_size)]));
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_index_size)] = true;
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_heap_size)] =
			Int64GetDatum(merge_size->heap_size);
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_heap_size)] = true;
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_toast_size)] =
			Int64GetDatum(merge_size->toast_size);
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_toast_size)] = true;
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_index_size)] =
			Int64GetDatum(merge_size->index_size);
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_index_size)] = true;
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_pre_compression)] =
			Int64GetDatum(merge_rowcnt_pre_compression +
						  DatumGetInt64(values[AttrNumberGetAttrOffset(
							  Anum_compression_chunk_size_numrows_pre_compression)]));
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_pre_compression)] = true;
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_post_compression)] =
			Int64GetDatum(merge_rowcnt_post_compression +
						  DatumGetInt64(values[AttrNumberGetAttrOffset(
							  Anum_compression_chunk_size_numrows_post_compression)]));
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_post_compression)] = true;

		new_tuple =
			heap_modify_tuple(tuple, ts_scanner_get_tupledesc(ti), values, replIsnull, repl);
		ts_catalog_update(ti->scanrel, new_tuple);
		heap_freetuple(new_tuple);

		if (should_free)
			heap_freetuple(tuple);

		updated = true;
		break;
	}

	ts_scan_iterator_end(&iterator);
	ts_scan_iterator_close(&iterator);
	return updated;
}

static void
get_hypertable_or_cagg_name(Hypertable *ht, Name objname)
{
	ContinuousAggHypertableStatus status = ts_continuous_agg_hypertable_status(ht->fd.id);
	if (status == HypertableIsNotContinuousAgg || status == HypertableIsRawTable)
		namestrcpy(objname, NameStr(ht->fd.table_name));
	else if (status == HypertableIsMaterialization)
	{
		ContinuousAgg *cagg = ts_continuous_agg_find_by_mat_hypertable_id(ht->fd.id);
		namestrcpy(objname, NameStr(cagg->data.user_view_name));
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unexpected hypertable status for %s %d",
						NameStr(ht->fd.table_name),
						status)));
	}
}

static void
compresschunkcxt_init(CompressChunkCxt *cxt, Cache *hcache, Oid hypertable_relid, Oid chunk_relid)
{
	Hypertable *srcht = ts_hypertable_cache_get_entry(hcache, hypertable_relid, CACHE_FLAG_NONE);
	Hypertable *compress_ht;
	Chunk *srcchunk;

	ts_hypertable_permissions_check(srcht->main_table_relid, GetUserId());

	if (!TS_HYPERTABLE_HAS_COMPRESSION_TABLE(srcht))
	{
		NameData cagg_ht_name;
		get_hypertable_or_cagg_name(srcht, &cagg_ht_name);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("compression not enabled on \"%s\"", NameStr(cagg_ht_name)),
				 errdetail("It is not possible to compress chunks on a hypertable or"
						   " continuous aggregate that does not have compression enabled."),
				 errhint("Enable compression using ALTER TABLE/MATERIALIZED VIEW with"
						 " the timescaledb.compress option.")));
	}
	compress_ht = ts_hypertable_get_by_id(srcht->fd.compressed_hypertable_id);
	if (compress_ht == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("missing compress hypertable")));
	/* user has to be the owner of the compression table too */
	ts_hypertable_permissions_check(compress_ht->main_table_relid, GetUserId());

	if (!srcht->space) /* something is wrong */
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("missing hyperspace for hypertable")));
	/* refetch the srcchunk with all attributes filled in */
	srcchunk = ts_chunk_get_by_relid(chunk_relid, true);
	ts_chunk_validate_chunk_status_for_operation(srcchunk, CHUNK_COMPRESS, true);
	cxt->srcht = srcht;
	cxt->compress_ht = compress_ht;
	cxt->srcht_chunk = srcchunk;
}

static Chunk *
find_chunk_to_merge_into(Hypertable *ht, Chunk *current_chunk)
{
	int64 max_chunk_interval, current_chunk_interval = 0, compressed_chunk_interval = 0;
	Chunk *previous_chunk;
	Point *p;

	const Dimension *time_dim = hyperspace_get_open_dimension(ht->space, 0);

	Assert(time_dim != NULL);

	if (time_dim->fd.compress_interval_length == 0)
		return NULL;

	Assert(current_chunk->cube->num_slices > 0);
	Assert(current_chunk->cube->slices[0]->fd.dimension_id == time_dim->fd.id);

	max_chunk_interval = time_dim->fd.compress_interval_length;

	p = ts_point_create(current_chunk->cube->num_slices);

	/* First coordinate is the time coordinates and we want it to fall into previous chunk
	 * hence we reduce it by 1
	 */
	p->coordinates[p->num_coords++] = current_chunk->cube->slices[0]->fd.range_start - 1;
	current_chunk_interval = current_chunk->cube->slices[0]->fd.range_end -
							 current_chunk->cube->slices[0]->fd.range_start;

	for (int i = p->num_coords; i < current_chunk->cube->num_slices; i++)
	{
		p->coordinates[p->num_coords++] = current_chunk->cube->slices[i]->fd.range_start;
	}

	previous_chunk = ts_hypertable_find_chunk_for_point(ht, p);

	/* If there is no previous adjacent chunk along the time dimension or
	 * if it hasn't been compressed yet, we can't merge.
	 */
	if (!previous_chunk || !OidIsValid(previous_chunk->fd.compressed_chunk_id))
		return NULL;

	Assert(previous_chunk->cube->num_slices > 0);
	Assert(previous_chunk->cube->slices[0]->fd.dimension_id == time_dim->fd.id);

	compressed_chunk_interval = previous_chunk->cube->slices[0]->fd.range_end -
								previous_chunk->cube->slices[0]->fd.range_start;

	/* If the slices do not match (except on time dimension), we cannot merge the chunks. */
	if (previous_chunk->cube->num_slices != current_chunk->cube->num_slices)
		return NULL;

	for (int i = 1; i < previous_chunk->cube->num_slices; i++)
	{
		if (previous_chunk->cube->slices[i]->fd.id != current_chunk->cube->slices[i]->fd.id)
		{
			return NULL;
		}
	}

	/* If the compressed chunk is full, we can't merge any more. */
	if (compressed_chunk_interval == 0 ||
		compressed_chunk_interval + current_chunk_interval > max_chunk_interval)
		return NULL;

	return previous_chunk;
}

/* Check if compression order is violated by merging in a new chunk
 * Because data merged in uses higher sequence numbers than any data already in the chunk,
 * the only way the order is guaranteed can be if we know the data we are merging in would come
 * after the existing data according to the compression order. This is true if the data being merged
 * in has timestamps greater than the existing data and the first column in the order by is time
 * ASC.
 */
static bool
check_is_chunk_order_violated_by_merge(
	const Dimension *time_dim, Chunk *mergable_chunk, Chunk *compressed_chunk,
	const FormData_hypertable_compression **column_compression_info, int num_compression_infos)
{
	const DimensionSlice *mergable_slice =
		ts_hypercube_get_slice_by_dimension_id(mergable_chunk->cube, time_dim->fd.id);
	if (!mergable_slice)
		elog(ERROR, "mergable chunk has no time dimension slice");
	const DimensionSlice *compressed_slice =
		ts_hypercube_get_slice_by_dimension_id(compressed_chunk->cube, time_dim->fd.id);
	if (!compressed_slice)
		elog(ERROR, "compressed chunk has no time dimension slice");

	if (mergable_slice->fd.range_start > compressed_slice->fd.range_start &&
		mergable_slice->fd.range_end > compressed_slice->fd.range_start)
	{
		return true;
	}

	for (int i = 0; i < num_compression_infos; i++)
	{
		if (column_compression_info[i]->orderby_column_index == 1)
		{
			if (!column_compression_info[i]->orderby_asc)
			{
				return true;
			}
			if (get_attnum(time_dim->main_table_relid,
						   NameStr(column_compression_info[i]->attname)) != time_dim->column_attno)
			{
				return true;
			}
		}
	}

	return false;
}

static Oid
compress_chunk_impl(Oid hypertable_relid, Oid chunk_relid)
{
	Oid result_chunk_id = chunk_relid;
	CompressChunkCxt cxt;
	Chunk *compress_ht_chunk, *mergable_chunk;
	Cache *hcache;
	ListCell *lc;
	List *htcols_list = NIL;
	const ColumnCompressionInfo **colinfo_array;
	int i = 0, htcols_listlen;
	RelationSize before_size, after_size;
	CompressionStats cstat;
	bool new_compressed_chunk = false;

	hcache = ts_hypertable_cache_pin();
	compresschunkcxt_init(&cxt, hcache, hypertable_relid, chunk_relid);

	/* acquire locks on src and compress hypertable and src chunk */
	LockRelationOid(cxt.srcht->main_table_relid, AccessShareLock);
	LockRelationOid(cxt.compress_ht->main_table_relid, AccessShareLock);
	LockRelationOid(cxt.srcht_chunk->table_id, ExclusiveLock);

	/* acquire locks on catalog tables to keep till end of txn */
	LockRelationOid(catalog_get_table_id(ts_catalog_get(), HYPERTABLE_COMPRESSION),
					AccessShareLock);
	LockRelationOid(catalog_get_table_id(ts_catalog_get(), CHUNK), RowExclusiveLock);

	DEBUG_WAITPOINT("compress_chunk_impl_start");

	/*
	 * Re-read the state of the chunk after all locks have been acquired and ensure
	 * it is still uncompressed. Another process running in parallel might have
	 * already performed the compression while we were waiting for the locks to be
	 * acquired.
	 */
	Chunk *chunk_state_after_lock = ts_chunk_get_by_relid(chunk_relid, true);

	/* Throw error if chunk has invalid status for operation */
	ts_chunk_validate_chunk_status_for_operation(chunk_state_after_lock, CHUNK_COMPRESS, true);

	/* get compression properties for hypertable */
	htcols_list = ts_hypertable_compression_get(cxt.srcht->fd.id);
	htcols_listlen = list_length(htcols_list);
	mergable_chunk = find_chunk_to_merge_into(cxt.srcht, cxt.srcht_chunk);
	if (!mergable_chunk)
	{
		/* create compressed chunk and a new table */
		compress_ht_chunk = create_compress_chunk(cxt.compress_ht, cxt.srcht_chunk, InvalidOid);
		new_compressed_chunk = true;
	}
	else
	{
		/* use an existing compressed chunk to compress into */
		compress_ht_chunk = ts_chunk_get_by_id(mergable_chunk->fd.compressed_chunk_id, true);
		result_chunk_id = mergable_chunk->table_id;
	}
	/* convert list to array of pointers for compress_chunk */
	colinfo_array = palloc(sizeof(ColumnCompressionInfo *) * htcols_listlen);
	foreach (lc, htcols_list)
	{
		FormData_hypertable_compression *fd = (FormData_hypertable_compression *) lfirst(lc);
		colinfo_array[i++] = fd;
	}
	before_size = ts_relation_size_impl(cxt.srcht_chunk->table_id);
	cstat = compress_chunk(cxt.srcht_chunk->table_id,
						   compress_ht_chunk->table_id,
						   colinfo_array,
						   htcols_listlen);

	/* Drop all FK constraints on the uncompressed chunk. This is needed to allow
	 * cascading deleted data in FK-referenced tables, while blocking deleting data
	 * directly on the hypertable or chunks.
	 */
	ts_chunk_drop_fks(cxt.srcht_chunk);
	after_size = ts_relation_size_impl(compress_ht_chunk->table_id);

	if (new_compressed_chunk)
	{
		compression_chunk_size_catalog_insert(cxt.srcht_chunk->fd.id,
											  &before_size,
											  compress_ht_chunk->fd.id,
											  &after_size,
											  cstat.rowcnt_pre_compression,
											  cstat.rowcnt_post_compression);

		/* Copy chunk constraints (including fkey) to compressed chunk.
		 * Do this after compressing the chunk to avoid holding strong, unnecessary locks on the
		 * referenced table during compression.
		 */
		ts_chunk_constraints_create(cxt.compress_ht, compress_ht_chunk);
		ts_trigger_create_all_on_chunk(compress_ht_chunk);
		ts_chunk_set_compressed_chunk(cxt.srcht_chunk, compress_ht_chunk->fd.id);
	}
	else
	{
		compression_chunk_size_catalog_update_merged(mergable_chunk->fd.id,
													 &before_size,
													 compress_ht_chunk->fd.id,
													 &after_size,
													 cstat.rowcnt_pre_compression,
													 cstat.rowcnt_post_compression);

		const Dimension *time_dim = hyperspace_get_open_dimension(cxt.srcht->space, 0);
		Assert(time_dim != NULL);

		bool chunk_unordered = check_is_chunk_order_violated_by_merge(time_dim,
																	  mergable_chunk,
																	  cxt.srcht_chunk,
																	  colinfo_array,
																	  htcols_listlen);

		ts_chunk_merge_on_dimension(cxt.srcht, mergable_chunk, cxt.srcht_chunk, time_dim->fd.id);

		if (chunk_unordered)
		{
			ts_chunk_set_unordered(mergable_chunk);
			tsl_recompress_chunk_wrapper(mergable_chunk);
		}
	}

	ts_cache_release(hcache);
	return result_chunk_id;
}

static bool
decompress_chunk_impl(Oid uncompressed_hypertable_relid, Oid uncompressed_chunk_relid,
					  bool if_compressed)
{
	Cache *hcache;
	Hypertable *uncompressed_hypertable =
		ts_hypertable_cache_get_cache_and_entry(uncompressed_hypertable_relid,
												CACHE_FLAG_NONE,
												&hcache);
	Hypertable *compressed_hypertable;
	Chunk *uncompressed_chunk;
	Chunk *compressed_chunk;

	ts_hypertable_permissions_check(uncompressed_hypertable->main_table_relid, GetUserId());

	compressed_hypertable =
		ts_hypertable_get_by_id(uncompressed_hypertable->fd.compressed_hypertable_id);
	if (compressed_hypertable == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("missing compressed hypertable")));

	uncompressed_chunk = ts_chunk_get_by_relid(uncompressed_chunk_relid, true);
	if (uncompressed_chunk == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("table \"%s\" is not a chunk", get_rel_name(uncompressed_chunk_relid))));

	if (uncompressed_chunk->fd.hypertable_id != uncompressed_hypertable->fd.id)
		elog(ERROR, "hypertable and chunk do not match");

	if (uncompressed_chunk->fd.compressed_chunk_id == INVALID_CHUNK_ID)
	{
		ts_cache_release(hcache);
		ereport((if_compressed ? NOTICE : ERROR),
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("chunk \"%s\" is not compressed", get_rel_name(uncompressed_chunk_relid))));
		return false;
	}

	ts_chunk_validate_chunk_status_for_operation(uncompressed_chunk, CHUNK_DECOMPRESS, true);
	compressed_chunk = ts_chunk_get_by_id(uncompressed_chunk->fd.compressed_chunk_id, true);

	/* acquire locks on src and compress hypertable and src chunk */
	LockRelationOid(uncompressed_hypertable->main_table_relid, AccessShareLock);
	LockRelationOid(compressed_hypertable->main_table_relid, AccessShareLock);

	/*
	 * Acquire an ExclusiveLock on the uncompressed and the compressed
	 * chunk (the chunks can still be accessed by reads).
	 *
	 * The lock on the compressed chunk is needed because it gets deleted
	 * after decompression. The lock on the uncompressed chunk is needed
	 * to avoid deadlocks (e.g., caused by later lock upgrades or parallel
	 * started chunk compressions).
	 *
	 * Note: Also the function decompress_chunk() will request an
	 *       ExclusiveLock on the compressed and on the uncompressed
	 *       chunk. See the comments in function about the concurrency of
	 *       operations.
	 */
	LockRelationOid(uncompressed_chunk->table_id, ExclusiveLock);
	LockRelationOid(compressed_chunk->table_id, ExclusiveLock);

	/* acquire locks on catalog tables to keep till end of txn */
	LockRelationOid(catalog_get_table_id(ts_catalog_get(), HYPERTABLE_COMPRESSION),
					AccessShareLock);

	LockRelationOid(catalog_get_table_id(ts_catalog_get(), CHUNK), RowExclusiveLock);

	DEBUG_WAITPOINT("decompress_chunk_impl_start");

	/*
	 * Re-read the state of the chunk after all locks have been acquired and ensure
	 * it is still compressed. Another process running in parallel might have
	 * already performed the decompression while we were waiting for the locks to be
	 * acquired.
	 */
	Chunk *chunk_state_after_lock = ts_chunk_get_by_relid(uncompressed_chunk_relid, true);

	/* Throw error if chunk has invalid status for operation */
	ts_chunk_validate_chunk_status_for_operation(chunk_state_after_lock, CHUNK_DECOMPRESS, true);

	decompress_chunk(compressed_chunk->table_id, uncompressed_chunk->table_id);

	/* Recreate FK constraints, since they were dropped during compression. */
	ts_chunk_create_fks(uncompressed_hypertable, uncompressed_chunk);

	/* Delete the compressed chunk */
	ts_compression_chunk_size_delete(uncompressed_chunk->fd.id);
	ts_chunk_clear_compressed_chunk(uncompressed_chunk);

	/*
	 * Lock the compressed chunk that is going to be deleted. At this point,
	 * the reference to the compressed chunk is already removed from the
	 * catalog. So, new readers do not include it in their operations.
	 *
	 * Note: Calling performMultipleDeletions in chunk_index_tuple_delete
	 * also requests an AccessExclusiveLock on the compressed_chunk. However,
	 * this call makes the lock on the chunk explicit.
	 */
	LockRelationOid(compressed_chunk->table_id, AccessExclusiveLock);
	ts_chunk_drop(compressed_chunk, DROP_RESTRICT, -1);
	ts_cache_release(hcache);
	return true;
}

/*
 * Set if_not_compressed to true for idempotent operation. Aborts transaction if the chunk is
 * already compressed, unless it is running in idempotent mode.
 */

Oid
tsl_compress_chunk_wrapper(Chunk *chunk, bool if_not_compressed)
{
	if (chunk->fd.compressed_chunk_id != INVALID_CHUNK_ID)
	{
		ereport((if_not_compressed ? NOTICE : ERROR),
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("chunk \"%s\" is already compressed", get_rel_name(chunk->table_id))));
		return chunk->table_id;
	}

	return compress_chunk_impl(chunk->hypertable_relid, chunk->table_id);
}

/*
 * Helper for remote invocation of chunk compression and decompression.
 */
static bool
invoke_compression_func_remotely(FunctionCallInfo fcinfo, const Chunk *chunk)
{
	List *datanodes;
	DistCmdResult *distres;
	bool isnull_result = true;
	Size i;

	Assert(chunk->relkind == RELKIND_FOREIGN_TABLE);
	Assert(chunk->data_nodes != NIL);
	datanodes = ts_chunk_get_data_node_name_list(chunk);
	distres = ts_dist_cmd_invoke_func_call_on_data_nodes(fcinfo, datanodes);

	for (i = 0; i < ts_dist_cmd_response_count(distres); i++)
	{
		const char *node_name;
		bool isnull;
		Datum PG_USED_FOR_ASSERTS_ONLY d;

		d = ts_dist_cmd_get_single_scalar_result_by_index(distres, i, &isnull, &node_name);

		/* Make sure data nodes either (1) all return NULL, or (2) all return
		 * a non-null result. */
		if (i > 0 && isnull_result != isnull)
			elog(ERROR, "inconsistent result from data node \"%s\"", node_name);

		isnull_result = isnull;

		if (!isnull)
		{
			Assert(OidIsValid(DatumGetObjectId(d)));
		}
	}

	ts_dist_cmd_close_response(distres);

	return !isnull_result;
}

static bool
compress_remote_chunk(FunctionCallInfo fcinfo, const Chunk *chunk, bool if_not_compressed)
{
	bool success = invoke_compression_func_remotely(fcinfo, chunk);

	if (!success)
		ereport((if_not_compressed ? NOTICE : ERROR),
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("chunk \"%s\" is already compressed", get_rel_name(chunk->table_id))));

	return success;
}

static bool
decompress_remote_chunk(FunctionCallInfo fcinfo, const Chunk *chunk, bool if_compressed)
{
	bool success = invoke_compression_func_remotely(fcinfo, chunk);

	if (!success)
		ereport((if_compressed ? NOTICE : ERROR),
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("chunk \"%s\" is not compressed", get_rel_name(chunk->table_id))));

	return success;
}

/*
 * Create a new compressed chunk using existing table with compressed data.
 *
 * chunk_relid - non-compressed chunk relid
 * chunk_table - table containing compressed data
 */
Datum
tsl_create_compressed_chunk(PG_FUNCTION_ARGS)
{
	Oid chunk_relid = PG_GETARG_OID(0);
	Oid chunk_table = PG_GETARG_OID(1);
	RelationSize uncompressed_size = { .heap_size = PG_GETARG_INT64(2),
									   .toast_size = PG_GETARG_INT64(3),
									   .index_size = PG_GETARG_INT64(4) };
	RelationSize compressed_size = { .heap_size = PG_GETARG_INT64(5),
									 .toast_size = PG_GETARG_INT64(6),
									 .index_size = PG_GETARG_INT64(7) };
	int64 numrows_pre_compression = PG_GETARG_INT64(8);
	int64 numrows_post_compression = PG_GETARG_INT64(9);
	Chunk *chunk;
	Chunk *compress_ht_chunk;
	Cache *hcache;
	CompressChunkCxt cxt;
	bool chunk_was_compressed;

	Assert(!PG_ARGISNULL(0));
	Assert(!PG_ARGISNULL(1));

	ts_feature_flag_check(FEATURE_HYPERTABLE_COMPRESSION);
	TS_PREVENT_FUNC_IF_READ_ONLY();

	chunk = ts_chunk_get_by_relid(chunk_relid, true);
	hcache = ts_hypertable_cache_pin();
	compresschunkcxt_init(&cxt, hcache, chunk->hypertable_relid, chunk_relid);

	/* Acquire locks on src and compress hypertable and src chunk */
	LockRelationOid(cxt.srcht->main_table_relid, AccessShareLock);
	LockRelationOid(cxt.compress_ht->main_table_relid, AccessShareLock);
	LockRelationOid(cxt.srcht_chunk->table_id, ShareLock);

	/* Aquire locks on catalog tables to keep till end of txn */
	LockRelationOid(catalog_get_table_id(ts_catalog_get(), HYPERTABLE_COMPRESSION),
					AccessShareLock);
	LockRelationOid(catalog_get_table_id(ts_catalog_get(), CHUNK), RowExclusiveLock);

	/* Create compressed chunk using existing table */
	compress_ht_chunk = create_compress_chunk(cxt.compress_ht, cxt.srcht_chunk, chunk_table);

	/* Copy chunk constraints (including fkey) to compressed chunk */
	ts_chunk_constraints_create(cxt.compress_ht, compress_ht_chunk);
	ts_trigger_create_all_on_chunk(compress_ht_chunk);

	/* Drop all FK constraints on the uncompressed chunk. This is needed to allow
	 * cascading deleted data in FK-referenced tables, while blocking deleting data
	 * directly on the hypertable or chunks.
	 */
	ts_chunk_drop_fks(cxt.srcht_chunk);

	/* Insert empty stats to compression_chunk_size */
	compression_chunk_size_catalog_insert(cxt.srcht_chunk->fd.id,
										  &uncompressed_size,
										  compress_ht_chunk->fd.id,
										  &compressed_size,
										  numrows_pre_compression,
										  numrows_post_compression);

	chunk_was_compressed = ts_chunk_is_compressed(cxt.srcht_chunk);
	ts_chunk_set_compressed_chunk(cxt.srcht_chunk, compress_ht_chunk->fd.id);
	if (!chunk_was_compressed && ts_table_has_tuples(cxt.srcht_chunk->table_id, AccessShareLock))
	{
		/* The chunk was not compressed before it had the compressed chunk
		 * attached to it, and it contains rows, so we set it to be partial.
		 */
		ts_chunk_set_partial(cxt.srcht_chunk);
	}
	ts_cache_release(hcache);

	PG_RETURN_OID(chunk_relid);
}

Datum
tsl_compress_chunk(PG_FUNCTION_ARGS)
{
	Oid uncompressed_chunk_id = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	bool if_not_compressed = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);

	ts_feature_flag_check(FEATURE_HYPERTABLE_COMPRESSION);

	TS_PREVENT_FUNC_IF_READ_ONLY();
	Chunk *chunk = ts_chunk_get_by_relid(uncompressed_chunk_id, true);

	if (chunk->relkind == RELKIND_FOREIGN_TABLE)
	{
		/* chunks of distributed hypertables are foreign tables */
		if (!compress_remote_chunk(fcinfo, chunk, if_not_compressed))
			PG_RETURN_NULL();

		/*
		 * Updating the chunk compression status of the Access Node AFTER executing remote
		 * compression. In the event of failure, the compressed status will NOT be set. The
		 * distributed compression policy will attempt to compress again, which is idempotent, thus
		 * the metadata are eventually consistent.
		 */
		ts_chunk_set_compressed_chunk(chunk, INVALID_CHUNK_ID);
	}
	else
	{
		uncompressed_chunk_id = tsl_compress_chunk_wrapper(chunk, if_not_compressed);
	}

	PG_RETURN_OID(uncompressed_chunk_id);
}

Datum
tsl_decompress_chunk(PG_FUNCTION_ARGS)
{
	Oid uncompressed_chunk_id = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	bool if_compressed = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);

	ts_feature_flag_check(FEATURE_HYPERTABLE_COMPRESSION);

	TS_PREVENT_FUNC_IF_READ_ONLY();
	Chunk *uncompressed_chunk = ts_chunk_get_by_relid(uncompressed_chunk_id, true);

	if (NULL == uncompressed_chunk)
		elog(ERROR, "unknown chunk id %d", uncompressed_chunk_id);

	if (uncompressed_chunk->relkind == RELKIND_FOREIGN_TABLE)
	{
		/*
		 * Updating the chunk compression status of the Access Node BEFORE executing remote
		 * decompression. In the event of failure, the compressed status will be cleared. The
		 * distributed compression policy will attempt to compress again, which is idempotent, thus
		 * the metadata are eventually consistent.
		 * If CHUNK_STATUS_COMPRESSED is cleared, then it is probable that a remote compress_chunk()
		 * has not taken place, but not certain. For this above reason, this flag should not be
		 * assumed to be consistent (when it is cleared) for Access-Nodes. When used in distributed
		 * hypertables one should take advantage of the idempotent properties of remote
		 * compress_chunk() and distributed compression policy to make progress.
		 */
		ts_chunk_clear_compressed_chunk(uncompressed_chunk);

		if (!decompress_remote_chunk(fcinfo, uncompressed_chunk, if_compressed))
			PG_RETURN_NULL();

		PG_RETURN_OID(uncompressed_chunk_id);
	}

	if (!decompress_chunk_impl(uncompressed_chunk->hypertable_relid,
							   uncompressed_chunk_id,
							   if_compressed))
		PG_RETURN_NULL();

	PG_RETURN_OID(uncompressed_chunk_id);
}

bool
tsl_recompress_chunk_wrapper(Chunk *uncompressed_chunk)
{
	Oid uncompressed_chunk_relid = uncompressed_chunk->table_id;
	if (ts_chunk_is_unordered(uncompressed_chunk))
	{
		if (!decompress_chunk_impl(uncompressed_chunk->hypertable_relid,
								   uncompressed_chunk_relid,
								   false))
			return false;
	}
	Chunk *chunk = ts_chunk_get_by_relid(uncompressed_chunk_relid, true);
	Assert(!ts_chunk_is_compressed(chunk));
	tsl_compress_chunk_wrapper(chunk, false);
	return true;
}

static bool
recompress_remote_chunk(FunctionCallInfo fcinfo, Chunk *chunk)
{
	return invoke_compression_func_remotely(fcinfo, chunk);
}

/*
 * This is hacky but it doesn't matter. We just want to check for the existence of such an index
 * on the compressed chunk. For distributed hypertables, returning the index oid would raise issues,
 * because the Access Node does not see that oid. So we return the oid of the uncompresed chunk
 * instead, when an index is found.
 */
extern Datum
tsl_get_compressed_chunk_index_for_recompression(PG_FUNCTION_ARGS)
{
	Oid uncompressed_chunk_id = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	Chunk *uncompressed_chunk = ts_chunk_get_by_relid(uncompressed_chunk_id, true);

	ts_feature_flag_check(FEATURE_HYPERTABLE_COMPRESSION);
	if (NULL == uncompressed_chunk)
		elog(ERROR, "unknown chunk id %d", uncompressed_chunk_id);

	/* push down to data nodes for distributed case */
	if (uncompressed_chunk->relkind == RELKIND_FOREIGN_TABLE)
	{
		/* look for index on data nodes */
		if (!(invoke_compression_func_remotely(fcinfo, uncompressed_chunk)))
			PG_RETURN_NULL();
		else // don't care what the idx oid is, data node will find it and open it
			PG_RETURN_OID(uncompressed_chunk_id);
	}
	int32 srcht_id = uncompressed_chunk->fd.hypertable_id;
	Chunk *compressed_chunk = ts_chunk_get_by_id(uncompressed_chunk->fd.compressed_chunk_id, true);

	List *htcols_list = ts_hypertable_compression_get(srcht_id);
	ListCell *lc;
	int htcols_listlen = list_length(htcols_list);

	const ColumnCompressionInfo **colinfo_array;
	colinfo_array = palloc(sizeof(ColumnCompressionInfo *) * htcols_listlen);

	int i = 0;

	foreach (lc, htcols_list)
	{
		FormData_hypertable_compression *fd = (FormData_hypertable_compression *) lfirst(lc);
		colinfo_array[i++] = fd;
	}

	const ColumnCompressionInfo **keys;
	int n_keys;
	int16 *in_column_offsets = compress_chunk_populate_keys(uncompressed_chunk->table_id,
															colinfo_array,
															htcols_listlen,
															&n_keys,
															&keys);

	Relation uncompressed_chunk_rel = table_open(uncompressed_chunk->table_id, ExclusiveLock);
	Relation compressed_chunk_rel = table_open(compressed_chunk->table_id, ExclusiveLock);
	TupleDesc compressed_rel_tupdesc = RelationGetDescr(compressed_chunk_rel);
	TupleDesc uncompressed_rel_tupdesc = RelationGetDescr(uncompressed_chunk_rel);

	RowCompressor row_compressor;
	row_compressor_init(&row_compressor,
						uncompressed_rel_tupdesc,
						compressed_chunk_rel,
						htcols_listlen,
						colinfo_array,
						in_column_offsets,
						compressed_rel_tupdesc->natts,
						true /*need_bistate*/,
						true /*reset_sequence*/,
						RECOMPRESS);

	/*
	 * Keep the ExclusiveLock on the compressed chunk. This lock will be requested
	 * by recompression later on, both in the case of segmentwise recompression, and
	 * in the case of decompress-compress. This implicitly locks the index too, so
	 * it cannot be dropped in another session, which is what we want to prevent by
	 * locking the compressed chunk here
	 */
	table_close(compressed_chunk_rel, NoLock);
	table_close(uncompressed_chunk_rel, NoLock);

	row_compressor_finish(&row_compressor);

	if (OidIsValid(row_compressor.index_oid))
	{
		PG_RETURN_OID(uncompressed_chunk_id);
	}
	else
		PG_RETURN_NULL();
}

/*
 * Recompress an existing chunk by decompressing the batches
 * that are affected by the addition of newer data. The existing
 * compressed chunk will not be recreated but modified in place.
 *
 * 0 uncompressed_chunk_id REGCLASS
 * 1 if_not_compressed BOOL = false
 */
Datum
tsl_recompress_chunk_segmentwise(PG_FUNCTION_ARGS)
{
	Oid uncompressed_chunk_id = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	bool if_not_compressed = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);

	ts_feature_flag_check(FEATURE_HYPERTABLE_COMPRESSION);
	TS_PREVENT_FUNC_IF_READ_ONLY();
	Chunk *uncompressed_chunk = ts_chunk_get_by_relid(uncompressed_chunk_id, true);

	/* need to push down to data nodes if this is an access node */
	if (uncompressed_chunk->relkind == RELKIND_FOREIGN_TABLE)
	{
		if (!recompress_remote_chunk(fcinfo, uncompressed_chunk))
			PG_RETURN_NULL();

		PG_RETURN_OID(uncompressed_chunk_id);
	}

	int32 status = uncompressed_chunk->fd.status;

	if (status == CHUNK_STATUS_DEFAULT)
		elog(ERROR, "call compress_chunk instead of recompress_chunk");
	if (status == CHUNK_STATUS_COMPRESSED)
	{
		int elevel = if_not_compressed ? NOTICE : ERROR;
		elog(elevel,
			 "nothing to recompress in chunk %s.%s",
			 uncompressed_chunk->fd.schema_name.data,
			 uncompressed_chunk->fd.table_name.data);
	}
	/*
	 * only proceed if status in (3, 9, 11)
	 * 1: compressed
	 * 2: compressed_unordered
	 * 4: frozen
	 * 8: compressed_partial
	 */
	if (!(ts_chunk_is_compressed(uncompressed_chunk) &&
		  (ts_chunk_is_unordered(uncompressed_chunk) || ts_chunk_is_partial(uncompressed_chunk))))
		elog(ERROR,
			 "unexpected chunk status %d in chunk %s.%s",
			 status,
			 uncompressed_chunk->fd.schema_name.data,
			 uncompressed_chunk->fd.table_name.data);

	Chunk *compressed_chunk = ts_chunk_get_by_id(uncompressed_chunk->fd.compressed_chunk_id, true);

	/* new status after recompress should simply be compressed (1)
	 * It is ok to update this early on in the transaction as it keeps a lock
	 * on the updated tuple in the CHUNK table potentially preventing other transaction
	 * from updating it
	 */
	ts_chunk_clear_status(uncompressed_chunk,
						  CHUNK_STATUS_COMPRESSED_UNORDERED | CHUNK_STATUS_COMPRESSED_PARTIAL);

	/* lock both chunks, compresssed and uncompressed */
	/* TODO: Take RowExclusive locks instead of AccessExclusive */
	LockRelationOid(uncompressed_chunk->table_id, ExclusiveLock);
	LockRelationOid(compressed_chunk->table_id, ExclusiveLock);
	Relation compressed_chunk_rel = table_open(compressed_chunk->table_id, ExclusiveLock);
	Relation uncompressed_chunk_rel = table_open(uncompressed_chunk->table_id, ExclusiveLock);

	recompress_partial_chunks(uncompressed_chunk, compressed_chunk);

#if PG14_LT
	int options = 0;
#else
	ReindexParams params = { 0 };
	ReindexParams *options = &params;
#endif
	reindex_relation(compressed_chunk->table_id, 0, options);

	/* changed chunk status, so invalidate any plans involving this chunk */
	CacheInvalidateRelcacheByRelid(uncompressed_chunk_id);
	table_close(uncompressed_chunk_rel, ExclusiveLock);
	table_close(compressed_chunk_rel, ExclusiveLock);

	PG_RETURN_OID(uncompressed_chunk_id);
}
