#include <postgres.h>
#include <catalog/index.h>
#include <catalog/indexing.h>
#include <catalog/pg_index.h>
#include <catalog/pg_constraint.h>
#include <catalog/objectaddress.h>
#include <catalog/namespace.h>
#include <access/htup_details.h>
#include <utils/syscache.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/fmgroids.h>
#include <utils/builtins.h>
#include <nodes/parsenodes.h>
#include <commands/defrem.h>
#include <commands/tablecmds.h>

#include "chunk_index.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "catalog.h"
#include "scanner.h"
#include "chunk.h"

static List *
create_index_colnames(Relation indexrel)
{
	List	   *colnames = NIL;
	int			i;

	for (i = 0; i < indexrel->rd_att->natts; i++)
		colnames = lappend(colnames, pstrdup(NameStr(indexrel->rd_att->attrs[i]->attname)));

	return colnames;
}

/*
 * Get the relation's non-constraint indexes as a list of index OIDs.
 *
 * Constraint indexes are created as a result of creating the associated
 * constraint, so we want to exclude them when we create indexes on a chunk.
 */
static List *
relation_get_non_constraint_indexes(Relation rel)
{
	List	   *indexlist = RelationGetIndexList(rel);
	Relation	conrel;
	SysScanDesc conscan;
	ScanKeyData skey[1];
	HeapTuple	htup;

	ScanKeyInit(&skey[0],
				Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(rel)));

	conrel = heap_open(ConstraintRelationId, AccessShareLock);
	conscan = systable_beginscan(conrel, ConstraintRelidIndexId, true,
								 NULL, 1, skey);

	while (HeapTupleIsValid(htup = systable_getnext(conscan)))
	{
		Form_pg_constraint conform = (Form_pg_constraint) GETSTRUCT(htup);

		indexlist = list_delete_oid(indexlist, ObjectIdGetDatum(conform->conindid));
	}

	systable_endscan(conscan);
	heap_close(conrel, AccessShareLock);

	return indexlist;
}

/*
 * Pick a name for a chunk index.
 *
 * The chunk's index name will the original index name prefixed with the chunk's
 * table name, modulo any conflict resolution we need to do.
 */
static char *
chunk_index_choose_name(const char *tabname, const char *main_index_name, Oid namespaceid)
{
	char		buf[10];
	char	   *label = NULL;
	char	   *idxname;
	int			n = 0;

	for (;;)
	{
		/* makeObjectName will ensure the index name fits within a NAME type */
		idxname = makeObjectName(tabname, main_index_name, label);

		if (!OidIsValid(get_relname_relid(idxname, namespaceid)))
			break;

		/* found a conflict, so try a new name component */
		pfree(idxname);
		snprintf(buf, sizeof(buf), "%d", ++n);
		label = buf;
	}

	return idxname;
}

/*
 * Translate a hypertable's index attribute numbers to match a chunk.
 *
 * A hypertable's IndexInfo for one of its indexes references the attributes
 * (columns) in the hypertable by number. These numbers might not be the same
 * for the corresponding attribute in one of its chunks. To be able to use an
 * IndexInfo from a hypertable's index to create a corresponding index on a
 * chunk, we need to translate the attribute numbers to match the chunk.
 */
static void
chunk_translate_attnos(IndexInfo *ii, Relation idxrel, Relation chunkrel)
{
	int			i,
				natts = 0;

	Assert(ii->ii_NumIndexAttrs == idxrel->rd_att->natts);

	for (i = 0; i < idxrel->rd_att->natts; i++)
	{
		bool		found = false;
		int			j;

		for (j = 0; j < chunkrel->rd_att->natts; j++)
		{
			if (strncmp(NameStr(idxrel->rd_att->attrs[i]->attname),
						NameStr(chunkrel->rd_att->attrs[j]->attname),
						NAMEDATALEN) == 0)
			{
				ii->ii_KeyAttrNumbers[natts++] = chunkrel->rd_att->attrs[j]->attnum;
				found = true;
				break;
			}
		}

		if (!found)
			elog(ERROR, "Index attribute %s not found in chunk",
				 NameStr(idxrel->rd_att->attrs[i]->attname));
	}
}

/*
 * Create a chunk index based on the configuration of the "parent" index.
 */
static Oid
chunk_relation_index_create(Relation hypertable_indexrel,
							Relation chunkrel,
							bool isconstraint)
{
	Oid			chunk_indexrelid = InvalidOid;
	const char *indexname;
	IndexInfo  *indexinfo = BuildIndexInfo(hypertable_indexrel);
	HeapTuple	tuple;
	bool		isnull;
	Datum		reloptions;
	Datum		indclass;
	oidvector  *indclassoid;
	List	   *colnames = create_index_colnames(hypertable_indexrel);

	/*
	 * Convert the IndexInfo's attnos to match the chunk instead of the
	 * hypertable
	 */
	chunk_translate_attnos(indexinfo, hypertable_indexrel, chunkrel);

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(RelationGetRelid(hypertable_indexrel)));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for index relation %u",
			 RelationGetRelid(hypertable_indexrel));

	reloptions = SysCacheGetAttr(RELOID, tuple,
								 Anum_pg_class_reloptions, &isnull);

	indclass = SysCacheGetAttr(INDEXRELID, hypertable_indexrel->rd_indextuple,
							   Anum_pg_index_indclass, &isnull);
	Assert(!isnull);
	indclassoid = (oidvector *) DatumGetPointer(indclass);

	indexname = chunk_index_choose_name(get_rel_name(RelationGetRelid(chunkrel)),
						 get_rel_name(RelationGetRelid(hypertable_indexrel)),
							  get_rel_namespace(RelationGetRelid(chunkrel)));

	chunk_indexrelid = index_create(chunkrel,
									indexname,
									InvalidOid,
									InvalidOid,
									indexinfo,
									colnames,
									hypertable_indexrel->rd_rel->relam,
								  hypertable_indexrel->rd_rel->reltablespace,
									hypertable_indexrel->rd_indcollation,
									indclassoid->values,
									hypertable_indexrel->rd_indoption,
									reloptions,
								 hypertable_indexrel->rd_index->indisprimary,
									isconstraint,
									false,		/* deferrable */
									false,		/* init deferred */
									false,		/* allow system table mods */
									false,		/* skip build */
									false,		/* concurrent */
									false,		/* is internal */
									false);		/* if not exists */

	ReleaseSysCache(tuple);

	return chunk_indexrelid;
}


static bool
chunk_index_insert_relation(Relation rel,
							int32 chunk_id,
							const char *chunk_index,
							int32 hypertable_id,
							const char *parent_index)
{
	TupleDesc	desc = RelationGetDescr(rel);
	Datum		values[Natts_chunk_index];
	bool		nulls[Natts_chunk_index] = {false};

	values[Anum_chunk_index_chunk_id - 1] = Int32GetDatum(chunk_id);
	values[Anum_chunk_index_index_name - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(chunk_index));
	values[Anum_chunk_index_hypertable_id - 1] = Int32GetDatum(hypertable_id);
	values[Anum_chunk_index_hypertable_index_name - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(parent_index));
	catalog_insert_values(rel, desc, values, nulls);

	return true;
}

/*
 * Add an parent-child index mapping to the catalog.
 */
static bool
chunk_index_insert(int32 chunk_id,
				   const char *chunk_index,
				   int32 hypertable_id,
				   const char *hypertable_index)
{
	Catalog    *catalog = catalog_get();
	Relation	rel;
	bool		result;

	rel = heap_open(catalog->tables[CHUNK_INDEX].id, RowExclusiveLock);
	result = chunk_index_insert_relation(rel, chunk_id, chunk_index, hypertable_id, hypertable_index);
	heap_close(rel, RowExclusiveLock);

	return result;
}

/*
 * Create a new chunk index as a child of a parent hypertable index.
 *
 * The chunk index is created based on the information from the parent index
 * relation. This function is typically called when a new chunk is created and
 * it should, for each hypertable index, have a corresponding index of its own.
 */
static Oid
chunk_index_create(int32 hypertable_id,
				   Relation hypertable_idxrel,
				   int32 chunk_id,
				   Relation chunkrel,
				   bool isconstraint)
{
	Oid			chunk_indexrelid;

	chunk_indexrelid = chunk_relation_index_create(hypertable_idxrel, chunkrel, isconstraint);
	chunk_index_insert(chunk_id,
					   get_rel_name(chunk_indexrelid),
					   hypertable_id,
					   get_rel_name(RelationGetRelid(hypertable_idxrel)));

	return chunk_indexrelid;
}

/*
 * Create a new chunk index as a child of a parent hypertable index.
 *
 * The chunk index is created based on the statement that also creates the
 * parent index. This function is typically called when a new index is created
 * on the hypertable and we must add a corresponding index to each chunk.
 */
Oid
chunk_index_create_from_stmt(IndexStmt *stmt,
							 int32 chunk_id,
							 Oid chunkrelid,
							 int32 hypertable_id,
							 Oid hypertable_indexrelid)
{
	ObjectAddress idxobj;
	char	   *hypertable_indexname = get_rel_name(hypertable_indexrelid);

	if (NULL != stmt->idxname)
		stmt->idxname = chunk_index_choose_name(get_rel_name(chunkrelid),
												hypertable_indexname,
											  get_rel_namespace(chunkrelid));

	idxobj = DefineIndex(chunkrelid, stmt, InvalidOid, false, true, false, true);

	chunk_index_insert(chunk_id,
					   get_rel_name(idxobj.objectId),
					   hypertable_id,
					   hypertable_indexname);

	return idxobj.objectId;
}

static inline Oid
chunk_index_get_schemaid(Form_chunk_index chunk_index)
{
	Chunk	   *chunk = chunk_get_by_id(chunk_index->chunk_id, 0, true);

	return get_namespace_oid(NameStr(chunk->fd.schema_name), false);
}

#define chunk_index_tuple_get_schema(tuple) \
	chunk_index_get_schema((FormData_chunk_index *) GETSTRUCT(tuple));

/*
 * Create all indexes on a chunk, given the indexes that exists on the chunk's
 * hypertable.
 */
void
chunk_index_create_all(int32 hypertable_id, Oid hypertable_relid, int32 chunk_id, Oid chunkrelid)
{
	Relation	htrel;
	Relation	chunkrel;
	List	   *indexlist;
	ListCell   *lc;

	htrel = relation_open(hypertable_relid, AccessShareLock);

	/* Need ShareLock on the heap relation we are creating indexes on */
	chunkrel = relation_open(chunkrelid, ShareLock);

	/*
	 * We should only add those indexes that aren't created from constraints,
	 * since those are added separately.
	 *
	 * Ideally, we should just be able to check the index relation's rd_index
	 * struct for the flags indisunique, indisprimary, indisexclusion to
	 * figure out if this is a constraint-supporting index. However,
	 * indisunique is true both for plain unique indexes and those created
	 * from constraints. Instead, we prune the main table's index list,
	 * removing those indexes that are supporting a constraint.
	 */
	indexlist = relation_get_non_constraint_indexes(htrel);

	foreach(lc, indexlist)
	{
		Relation	hypertable_idxrel = relation_open(lfirst_oid(lc), AccessShareLock);

		chunk_index_create(hypertable_id, hypertable_idxrel, chunk_id, chunkrel, false);
		relation_close(hypertable_idxrel, AccessShareLock);
	}

	relation_close(chunkrel, NoLock);
	relation_close(htrel, AccessShareLock);
}

static int
chunk_index_scan(int indexid, ScanKeyData scankey[], int nkeys,
				 tuple_found_func tuple_found, void *data, LOCKMODE lockmode)
{
	Catalog    *catalog = catalog_get();
	ScannerCtx	scanCtx = {
		.table = catalog->tables[CHUNK_INDEX].id,
		.index = catalog->tables[CHUNK_INDEX].index_ids[indexid],
		.scantype = ScannerTypeIndex,
		.nkeys = nkeys,
		.scankey = scankey,
		.tuple_found = tuple_found,
		.data = data,
		.lockmode = lockmode,
		.scandirection = ForwardScanDirection,
	};

	return scanner_scan(&scanCtx);
}

#define chunk_index_scan_update(idxid, scankey, nkeys, tuple_found, data)	\
	chunk_index_scan(idxid, scankey, nkeys, tuple_found, data, RowExclusiveLock)

static bool
chunk_index_tuple_delete(TupleInfo *ti, void *data)
{
	FormData_chunk_index *chunk_index = (FormData_chunk_index *) GETSTRUCT(ti->tuple);
	Oid			schemaid = chunk_index_get_schemaid(chunk_index);
	ObjectAddress idxobj = {
		.classId = RelationRelationId,
		.objectId = get_relname_relid(NameStr(chunk_index->index_name), schemaid),
	};
	bool	   *should_drop = data;

	catalog_delete(ti->scanrel, ti->tuple);

	if (*should_drop)
		performDeletion(&idxobj, DROP_RESTRICT, 0);

	return true;
}

int
chunk_index_delete_children_of(Hypertable *ht, Oid hypertable_indexrelid)
{
	ScanKeyData scankey[2];
	const char *indexname = get_rel_name(hypertable_indexrelid);
	bool		should_drop = true;		/* In addition to deleting metadata,
										 * we should also drop all children
										 * tables */

	ScanKeyInit(&scankey[0],
	  Anum_chunk_index_hypertable_id_hypertable_index_name_idx_hypertable_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(ht->fd.id));
	ScanKeyInit(&scankey[1],
				Anum_chunk_index_hypertable_id_hypertable_index_name_idx_hypertable_index_name,
				BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(indexname));

	return chunk_index_scan_update(CHUNK_INDEX_HYPERTABLE_ID_HYPERTABLE_INDEX_NAME_IDX,
						 scankey, 2, chunk_index_tuple_delete, &should_drop);
}

int
chunk_index_delete(Chunk *chunk, Oid chunk_indexrelid, bool drop_index)
{
	ScanKeyData scankey[2];
	const char *indexname = get_rel_name(chunk_indexrelid);

	ScanKeyInit(&scankey[0],
				Anum_chunk_index_chunk_id_index_name_idx_chunk_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(chunk->fd.id));
	ScanKeyInit(&scankey[1],
				Anum_chunk_index_chunk_id_index_name_idx_index_name,
				BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(indexname));

	return chunk_index_scan_update(CHUNK_INDEX_CHUNK_ID_INDEX_NAME_IDX,
						  scankey, 2, chunk_index_tuple_delete, &drop_index);
}

typedef struct ChunkIndexRenameInfo
{
	const char *oldname,
			   *newname;
	bool		isparent;
} ChunkIndexRenameInfo;

static bool
chunk_index_tuple_rename(TupleInfo *ti, void *data)
{
	ChunkIndexRenameInfo *info = data;
	HeapTuple	tuple = heap_copytuple(ti->tuple);
	FormData_chunk_index *chunk_index = (FormData_chunk_index *) GETSTRUCT(tuple);

	if (info->isparent)
	{
		/*
		 * If the renaming is for a hypertable index, we also rename all
		 * corresponding chunk indexes
		 */
		Chunk	   *chunk = chunk_get_by_id(chunk_index->chunk_id, 0, true);
		Oid			chunk_schemaoid = get_namespace_oid(NameStr(chunk->fd.schema_name), false);
		const char *chunk_index_name = chunk_index_choose_name(NameStr(chunk->fd.table_name),
															   info->newname,
															chunk_schemaoid);
		Oid			chunk_indexrelid = get_relname_relid(NameStr(chunk_index->index_name),
														 chunk_schemaoid);

		/* Update the metadata */
		namestrcpy(&chunk_index->index_name, chunk_index_name);
		namestrcpy(&chunk_index->hypertable_index_name, info->newname);

		/* Rename the chunk index */
		RenameRelationInternal(chunk_indexrelid, chunk_index_name, false);
	}
	else
		namestrcpy(&chunk_index->index_name, info->newname);

	catalog_update(ti->scanrel, tuple);
	heap_freetuple(tuple);

	if (info->isparent)
		return true;

	return false;
}

int
chunk_index_rename(Chunk *chunk, Oid chunk_indexrelid, const char *newname)
{
	ScanKeyData scankey[2];
	const char *indexname = get_rel_name(chunk_indexrelid);
	ChunkIndexRenameInfo renameinfo = {
		.oldname = indexname,
		.newname = newname,
	};

	ScanKeyInit(&scankey[0], Anum_chunk_index_chunk_id_index_name_idx_chunk_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(chunk->fd.id));
	ScanKeyInit(&scankey[1], Anum_chunk_index_chunk_id_index_name_idx_index_name,
				BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(indexname));

	return chunk_index_scan_update(CHUNK_INDEX_CHUNK_ID_INDEX_NAME_IDX,
						  scankey, 2, chunk_index_tuple_rename, &renameinfo);
}

int
chunk_index_rename_parent(Hypertable *ht, Oid hypertable_indexrelid, const char *newname)
{
	ScanKeyData scankey[2];
	const char *indexname = get_rel_name(hypertable_indexrelid);
	ChunkIndexRenameInfo renameinfo = {
		.oldname = indexname,
		.newname = newname,
		.isparent = true,
	};

	ScanKeyInit(&scankey[0],
	  Anum_chunk_index_hypertable_id_hypertable_index_name_idx_hypertable_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(ht->fd.id));
	ScanKeyInit(&scankey[1],
				Anum_chunk_index_hypertable_id_hypertable_index_name_idx_hypertable_index_name,
				BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(indexname));

	return chunk_index_scan_update(CHUNK_INDEX_HYPERTABLE_ID_HYPERTABLE_INDEX_NAME_IDX,
						  scankey, 2, chunk_index_tuple_rename, &renameinfo);
}

static bool
chunk_index_tuple_set_tablespace(TupleInfo *ti, void *data)
{
	char	   *tablespace = data;
	FormData_chunk_index *chunk_index = (FormData_chunk_index *) GETSTRUCT(ti->tuple);
	Oid			schemaoid = chunk_index_get_schemaid(chunk_index);
	Oid			indexrelid = get_relname_relid(NameStr(chunk_index->index_name), schemaoid);
	AlterTableCmd *cmd = makeNode(AlterTableCmd);
	List	   *cmds = NIL;

	cmd->subtype = AT_SetTableSpace;
	cmd->name = tablespace;
	cmds = lappend(cmds, cmd);

	AlterTableInternal(indexrelid, cmds, false);

	return true;
}

int
chunk_index_set_tablespace(Hypertable *ht, Oid hypertable_indexrelid, const char *tablespace)
{
	ScanKeyData scankey[2];
	char	   *indexname = get_rel_name(hypertable_indexrelid);

	ScanKeyInit(&scankey[0],
	  Anum_chunk_index_hypertable_id_hypertable_index_name_idx_hypertable_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(ht->fd.id));
	ScanKeyInit(&scankey[1],
				Anum_chunk_index_hypertable_id_hypertable_index_name_idx_hypertable_index_name,
				BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(indexname));

	return chunk_index_scan_update(CHUNK_INDEX_HYPERTABLE_ID_HYPERTABLE_INDEX_NAME_IDX,
								scankey, 2, chunk_index_tuple_set_tablespace,
								   (char *) tablespace);
}
