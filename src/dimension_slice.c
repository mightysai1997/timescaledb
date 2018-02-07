#include <stdlib.h>
#include <postgres.h>
#include <access/relscan.h>
#include <access/xact.h>
#include <access/heapam.h>
#include <utils/rel.h>
#include <catalog/indexing.h>
#include <funcapi.h>

#include "catalog.h"
#include "dimension_slice.h"
#include "hypertable.h"
#include "scanner.h"
#include "dimension.h"
#include "chunk_constraint.h"
#include "dimension_vector.h"


/* Put DIMENSION_SLICE_MAXVALUE point in same slice as DIMENSION_SLICE_MAXVALUE-1, always */
/* This avoids the problem with coord < range_end where coord and range_end is an int64 */
#define REMAP_LAST_COORDINATE(coord) ((coord==DIMENSION_SLICE_MAXVALUE) ? DIMENSION_SLICE_MAXVALUE-1 : coord)

static inline DimensionSlice *
dimension_slice_alloc(void)
{
	return palloc0(sizeof(DimensionSlice));
}

static inline DimensionSlice *
dimension_slice_from_form_data(Form_dimension_slice fd)
{
	DimensionSlice *slice = dimension_slice_alloc();

	memcpy(&slice->fd, fd, sizeof(FormData_dimension_slice));
	slice->storage_free = NULL;
	slice->storage = NULL;
	return slice;
}

static inline DimensionSlice *
dimension_slice_from_tuple(HeapTuple tuple)
{
	return dimension_slice_from_form_data((Form_dimension_slice) GETSTRUCT(tuple));
}

DimensionSlice *
dimension_slice_create(int dimension_id, int64 range_start, int64 range_end)
{
	DimensionSlice *slice = dimension_slice_alloc();

	slice->fd.dimension_id = dimension_id;
	slice->fd.range_start = range_start;
	slice->fd.range_end = range_end;

	return slice;
}

int
dimension_slice_cmp(const DimensionSlice *left, const DimensionSlice *right)
{
	if (left->fd.range_start == right->fd.range_start)
	{
		if (left->fd.range_end == right->fd.range_end)
			return 0;

		if (left->fd.range_end > right->fd.range_end)
			return 1;

		return -1;
	}

	if (left->fd.range_start > right->fd.range_start)
		return 1;

	return -1;
}


int
dimension_slice_cmp_coordinate(const DimensionSlice *slice, int64 coord)
{
	coord = REMAP_LAST_COORDINATE(coord);
	if (coord < slice->fd.range_start)
		return -1;

	if (coord >= slice->fd.range_end)
		return 1;

	return 0;
}

typedef struct DimensionSliceScanData
{
	DimensionVec *slices;
	int			limit;
}			DimensionSliceScanData;

static bool
dimension_vec_tuple_found(TupleInfo *ti, void *data)
{
	DimensionVec **slices = data;
	DimensionSlice *slice = dimension_slice_from_tuple(ti->tuple);

	*slices = dimension_vec_add_slice(slices, slice);

	return true;
}

static int
dimension_slice_scan_limit_internal(int indexid,
									ScanKeyData *scankey,
									int nkeys,
									tuple_found_func on_tuple_found,
									void *scandata,
									int limit,
									LOCKMODE lockmode)
{
	Catalog    *catalog = catalog_get();
	ScannerCtx	scanCtx = {
		.table = catalog->tables[DIMENSION_SLICE].id,
		.index = catalog->tables[DIMENSION_SLICE].index_ids[indexid],
		.nkeys = nkeys,
		.scankey = scankey,
		.data = scandata,
		.limit = limit,
		.tuple_found = on_tuple_found,
		.lockmode = lockmode,
		.scandirection = ForwardScanDirection,
	};

	return scanner_scan(&scanCtx);
}

/*
 * Scan for slices that enclose the coordinate in the given dimension.
 *
 * Returns a dimension vector of slices that enclose the coordinate.
 */
DimensionVec *
dimension_slice_scan_limit(int32 dimension_id, int64 coordinate, int limit)
{
	ScanKeyData scankey[3];
	DimensionVec *slices = dimension_vec_create(limit > 0 ? limit : DIMENSION_VEC_DEFAULT_SIZE);

	coordinate = REMAP_LAST_COORDINATE(coordinate);

	/*
	 * Perform an index scan for slices matching the dimension's ID and which
	 * enclose the coordinate.
	 */
	ScanKeyInit(&scankey[0], Anum_dimension_slice_dimension_id_range_start_range_end_idx_dimension_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(dimension_id));
	ScanKeyInit(&scankey[1], Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_start,
				BTLessEqualStrategyNumber, F_INT8LE, Int64GetDatum(coordinate));
	ScanKeyInit(&scankey[2], Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_end,
				BTGreaterStrategyNumber, F_INT8GT, Int64GetDatum(coordinate));

	dimension_slice_scan_limit_internal(DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX,
										scankey,
										3,
										dimension_vec_tuple_found,
										&slices,
										limit,
										AccessShareLock);

	return dimension_vec_sort(&slices);
}

/*
 * Scan for slices that collide/overlap with the given range.
 *
 * Returns a dimension vector of colliding slices.
 */
DimensionVec *
dimension_slice_collision_scan_limit(int32 dimension_id, int64 range_start, int64 range_end, int limit)
{
	ScanKeyData scankey[3];
	DimensionVec *slices = dimension_vec_create(limit > 0 ? limit : DIMENSION_VEC_DEFAULT_SIZE);

	ScanKeyInit(&scankey[0], Anum_dimension_slice_dimension_id_range_start_range_end_idx_dimension_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(dimension_id));
	ScanKeyInit(&scankey[1], Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_start,
				BTLessStrategyNumber, F_INT8LT, Int64GetDatum(range_end));
	ScanKeyInit(&scankey[2], Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_end,
				BTGreaterStrategyNumber, F_INT8GT, Int64GetDatum(range_start));

	dimension_slice_scan_limit_internal(DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX,
										scankey,
										3,
										dimension_vec_tuple_found,
										&slices,
										limit,
										AccessShareLock);

	return dimension_vec_sort(&slices);
}

DimensionVec *
dimension_slice_scan_by_dimension(int32 dimension_id, int limit)
{
	ScanKeyData scankey[1];
	DimensionVec *slices = dimension_vec_create(limit > 0 ? limit : DIMENSION_VEC_DEFAULT_SIZE);

	ScanKeyInit(&scankey[0], Anum_dimension_slice_dimension_id_range_start_range_end_idx_dimension_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(dimension_id));

	dimension_slice_scan_limit_internal(DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX,
										scankey,
										1,
										dimension_vec_tuple_found,
										&slices,
										limit,
										AccessShareLock);

	return dimension_vec_sort(&slices);
}

static bool
dimension_slice_tuple_delete(TupleInfo *ti, void *data)
{
	bool		isnull;
	Datum		dimension_slice_id = heap_getattr(ti->tuple, Anum_dimension_slice_id, ti->desc, &isnull);
	bool	   *delete_constraints = data;
	CatalogSecurityContext sec_ctx;

	Assert(!isnull);

	/* delete chunk constraints */
	if (NULL != delete_constraints && *delete_constraints)
		chunk_constraint_delete_by_dimension_slice_id(DatumGetInt32(dimension_slice_id));

	catalog_become_owner(catalog_get(), &sec_ctx);
	catalog_delete(ti->scanrel, ti->tuple);
	catalog_restore_user(&sec_ctx);

	return true;
}

int
dimension_slice_delete_by_dimension_id(int32 dimension_id, bool delete_constraints)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_dimension_slice_dimension_id_range_start_range_end_idx_dimension_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(dimension_id));

	return dimension_slice_scan_limit_internal(DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX,
											   scankey,
											   1,
											   dimension_slice_tuple_delete,
											   &delete_constraints,
											   0,
											   RowExclusiveLock);
}

int
dimension_slice_delete_by_id(int32 dimension_slice_id, bool delete_constraints)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_dimension_slice_id_idx_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(dimension_slice_id));

	return dimension_slice_scan_limit_internal(DIMENSION_SLICE_ID_IDX,
											   scankey,
											   1,
											   dimension_slice_tuple_delete,
											   &delete_constraints,
											   1,
											   RowExclusiveLock);
}

static bool
dimension_slice_fill(TupleInfo *ti, void *data)
{
	DimensionSlice **slice = data;

	memcpy(&(*slice)->fd, GETSTRUCT(ti->tuple), sizeof(FormData_dimension_slice));
	return false;
}

/*
 * Scan for an existing slice that exactly matches the given slice's dimension
 * and range. If a match is found, the given slice is updated with slice ID.
 */
DimensionSlice *
dimension_slice_scan_for_existing(DimensionSlice *slice)
{
	ScanKeyData scankey[3];

	ScanKeyInit(&scankey[0], Anum_dimension_slice_dimension_id_range_start_range_end_idx_dimension_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(slice->fd.dimension_id));
	ScanKeyInit(&scankey[1], Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_start,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(slice->fd.range_start));
	ScanKeyInit(&scankey[2], Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_end,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(slice->fd.range_end));

	dimension_slice_scan_limit_internal(DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX,
										scankey, 3, dimension_slice_fill, &slice, 1, AccessShareLock);

	return slice;
}

static bool
dimension_slice_tuple_found(TupleInfo *ti, void *data)
{
	DimensionSlice **slice = data;

	*slice = dimension_slice_from_tuple(ti->tuple);
	return false;
}

DimensionSlice *
dimension_slice_scan_by_id(int32 dimension_slice_id)
{
	DimensionSlice *slice = NULL;
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0], Anum_dimension_slice_id_idx_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(dimension_slice_id));

	dimension_slice_scan_limit_internal(DIMENSION_SLICE_ID_IDX,
										scankey,
										1,
										dimension_slice_tuple_found,
										&slice,
										1,
										AccessShareLock);

	return slice;
}

DimensionSlice *
dimension_slice_copy(const DimensionSlice *original)
{
	DimensionSlice *new = palloc(sizeof(DimensionSlice));

	Assert(original->storage == NULL);
	Assert(original->storage_free == NULL);

	memcpy(new, original, sizeof(DimensionSlice));
	return new;
}

/*
 * Check if two dimensions slices overlap by doing collision detection in one
 * dimension.
 *
 * Returns true if the slices collide, otherwise false.
 */
bool
dimension_slices_collide(DimensionSlice *slice1, DimensionSlice *slice2)
{
	Assert(slice1->fd.dimension_id == slice2->fd.dimension_id);

	return (slice1->fd.range_start < slice2->fd.range_end &&
			slice1->fd.range_end > slice2->fd.range_start);
}

/*
 * Check whether two slices are identical.
 *
 * We require by assertion that the slices are in the same dimension and we only
 * compare the ranges (i.e., the slice ID is not important for equality).
 *
 * Returns true if the slices have identical ranges, otherwise false.
 */
bool
dimension_slices_equal(DimensionSlice *slice1, DimensionSlice *slice2)
{
	Assert(slice1->fd.dimension_id == slice2->fd.dimension_id);

	return slice1->fd.range_start == slice2->fd.range_start &&
		slice1->fd.range_end == slice2->fd.range_end;
}

/*-
 * Cut a slice that collides with another slice. The coordinate is the point of
 * insertion, and determines which end of the slice to cut.
 *
 * Case where we cut "after" the coordinate:
 *
 * ' [-x--------]
 * '      [--------]
 *
 * Case where we cut "before" the coordinate:
 *
 * '      [------x--]
 * ' [--------]
 *
 * Returns true if the slice was cut, otherwise false.
 */
bool
dimension_slice_cut(DimensionSlice *to_cut, DimensionSlice *other, int64 coord)
{
	Assert(to_cut->fd.dimension_id == other->fd.dimension_id);

	coord = REMAP_LAST_COORDINATE(coord);

	if (other->fd.range_end <= coord &&
		other->fd.range_end > to_cut->fd.range_start)
	{
		/* Cut "before" the coordinate */
		to_cut->fd.range_start = other->fd.range_end;
		return true;
	}
	else if (other->fd.range_start > coord &&
			 other->fd.range_start < to_cut->fd.range_end)
	{
		/* Cut "after" the coordinate */
		to_cut->fd.range_end = other->fd.range_start;
		return true;
	}

	return false;
}

void
dimension_slice_free(DimensionSlice *slice)
{
	if (slice->storage_free != NULL)
		slice->storage_free(slice->storage);
	pfree(slice);
}

static bool
dimension_slice_insert_relation(Relation rel, DimensionSlice *slice)
{
	TupleDesc	desc = RelationGetDescr(rel);
	Datum		values[Natts_dimension_slice];
	bool		nulls[Natts_dimension_slice] = {false};
	CatalogSecurityContext sec_ctx;

	if (slice->fd.id > 0)
		/* Slice already exists in table */
		return false;

	catalog_become_owner(catalog_get(), &sec_ctx);
	memset(values, 0, sizeof(values));
	slice->fd.id = catalog_table_next_seq_id(catalog_get(), DIMENSION_SLICE);
	values[Anum_dimension_slice_id - 1] = Int32GetDatum(slice->fd.id);
	values[Anum_dimension_slice_dimension_id - 1] = Int32GetDatum(slice->fd.dimension_id);
	values[Anum_dimension_slice_range_start - 1] = Int64GetDatum(slice->fd.range_start);
	values[Anum_dimension_slice_range_end - 1] = Int64GetDatum(slice->fd.range_end);

	catalog_insert_values(rel, desc, values, nulls);
	catalog_restore_user(&sec_ctx);

	return true;
}

/*
 * Insert slices into the catalog.
 */
void
dimension_slice_insert_multi(DimensionSlice **slices, Size num_slices)
{
	Catalog    *catalog = catalog_get();
	Relation	rel;
	Size		i;

	rel = heap_open(catalog->tables[DIMENSION_SLICE].id, RowExclusiveLock);

	for (i = 0; i < num_slices; i++)
		dimension_slice_insert_relation(rel, slices[i]);

	heap_close(rel, RowExclusiveLock);
}
