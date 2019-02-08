/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <nodes/relation.h>
#include <parser/parsetree.h>
#include <optimizer/var.h>
#include <optimizer/restrictinfo.h>
#include <nodes/plannodes.h>
#include <optimizer/prep.h>
#include <nodes/nodeFuncs.h>

#include <catalog/pg_constraint.h>
#include <catalog/pg_inherits.h>
#include "compat.h"
#if PG96 || PG10 /* PG11 consolidates pg_foo_fn.h -> pg_foo.h */
#include <catalog/pg_constraint_fn.h>
#include <catalog/pg_inherits_fn.h>
#endif
#include <optimizer/pathnode.h>
#include <catalog/pg_type.h>
#include <utils/errcodes.h>

#include "plan_expand_hypertable.h"
#include "hypertable.h"
#include "hypertable_restrict_info.h"
#include "planner.h"
#include "planner_import.h"
#include "plan_ordered_append.h"
#include "guc.h"
#include "extension.h"
#include "chunk.h"
#include "extension_constants.h"

typedef struct CollectQualCtx
{
	PlannerInfo *root;
	RelOptInfo *rel;
	List *restrictions;
	FuncExpr *chunk_exclusion_func;
} CollectQualCtx;

static Oid chunk_exclusion_func = InvalidOid;
#define CHUNK_EXCL_FUNC_NAME "chunks_in"
static Oid ts_chunks_arg_types[] = { RECORDOID, INT4ARRAYOID };

static void
init_chunk_exclusion_func()
{
	if (chunk_exclusion_func == InvalidOid)
		chunk_exclusion_func = get_function_oid(CHUNK_EXCL_FUNC_NAME,
												INTERNAL_SCHEMA_NAME,
												lengthof(ts_chunks_arg_types),
												ts_chunks_arg_types);
	Assert(chunk_exclusion_func != InvalidOid);
}

static bool
is_chunk_exclusion_func(Node *node)
{
	if (IsA(node, FuncExpr))
	{
		FuncExpr *explicit_exclusion = (FuncExpr *) node;

		if (explicit_exclusion->funcid == chunk_exclusion_func)
			return true;
	}
	return false;
}

/* Since baserestrictinfo is not yet set by the planner, we have to derive
 * it ourselves. It's safe for us to miss some restrict info clauses (this
 * will just result in more chunks being included) so this does not need
 * to be as comprehensive as the PG native derivation. This is inspired
 * by the derivation in `deconstruct_recurse` in PG
 *
 * We also try to detect explicit chunk exclusion and validate it is properly used. If
 * explicit chunk exclusion function is detected we'll skip adding restrictions since
 * they will not be used anyway.
 *
 * This method is modifying the query tree by removing `chunks_in` function node
 * from the WHERE clause. This is done to prevent the function from making the
 * planner believe that an index-only scan is not possible (since function is
 * only used to exclude chunks we actually don't need that function to run so it
 * can be removed).
 */
static Node *
collect_quals_mutator(Node *node, CollectQualCtx *ctx)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, FromExpr))
	{
		FromExpr *f = (FromExpr *) node;
		ListCell *lc;
		ListCell *prev = NULL;
		ListCell *next = NULL;
		bool func_removed = false;

		for (lc = list_head((List *) f->quals); lc != NULL;)
		{
			Node *qual = (Node *) lfirst(lc);
			RestrictInfo *restrictinfo;
			Relids relids;
			bool belongs_to_cur_rel = false;

			next = lnext(lc);

			relids = pull_varnos(qual);

			belongs_to_cur_rel =
				bms_num_members(relids) == 1 && bms_is_member(ctx->rel->relid, relids);

			if (!belongs_to_cur_rel)
			{
				lc = next;
				continue;
			}

			if (is_chunk_exclusion_func(qual))
			{
				FuncExpr *func_expr = (FuncExpr *) qual;

				/* validation */
				if (ctx->chunk_exclusion_func != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("only one chunks_in call is allowed per hypertable")));

				Assert(func_expr->args->length == 2);
				if (!IsA(linitial(func_expr->args), Var))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg(
								 "first parameter for chunks_in function needs to be a record")));

				ctx->chunk_exclusion_func = func_expr;
			}

			if (ctx->chunk_exclusion_func == NULL)
			{
#if PG96
				restrictinfo =
					make_restrictinfo((Expr *) qual, true, false, false, relids, NULL, NULL);
#else
				restrictinfo = make_restrictinfo((Expr *) qual,
												 true,
												 false,
												 false,
												 ctx->root->qual_security_level,
												 relids,
												 NULL,
												 NULL);
#endif
				ctx->restrictions = lappend(ctx->restrictions, restrictinfo);
			}
			else if (!func_removed && ctx->chunk_exclusion_func != NULL)
			{
				f->quals = (Node *) list_delete_cell((List *) (f->quals), lc, prev);
				func_removed = true;
			}
			prev = lc;
			lc = next;
		}
		if (ctx->chunk_exclusion_func != NULL)
			return (Node *) f;
	}

	return expression_tree_mutator(node, collect_quals_mutator, ctx);
}

static List *
find_children_oids(HypertableRestrictInfo *hri, Hypertable *ht, LOCKMODE lockmode)
{
	List *result;

	/*
	 * Using the HRI only makes sense if we are not using all the chunks,
	 * otherwise using the cached inheritance hierarchy is faster.
	 */
	if (!ts_hypertable_restrict_info_has_restrictions(hri))
		return find_all_inheritors(ht->main_table_relid, lockmode, NULL);
	;

	/* always include parent again, just as find_all_inheritors does */
	result = list_make1_oid(ht->main_table_relid);

	/* add chunks */
	result = list_concat(result, ts_hypertable_restrict_info_get_chunk_oids(hri, ht, lockmode));
	return result;
}

static bool
should_order_append(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht, bool *reverse)
{
	/* check if optimizations are enabled */
	if (ts_guc_disable_optimizations || !ts_guc_enable_ordered_append)
		return false;

	/*
	 * only do this optimization for hypertables with 1 dimension and queries
	 * with an ORDER BY and LIMIT clause
	 */
	if (ht->space->num_dimensions != 1 || root->parse->sortClause == NIL ||
		root->limit_tuples == -1.0)
		return false;

	return ts_ordered_append_should_optimize(root, rel, ht, reverse);
}

bool
ts_plan_expand_hypertable_valid_hypertable(Hypertable *ht, Query *parse, Index rti,
										   RangeTblEntry *rte)
{
	if (ht == NULL ||
		/* inheritance enabled */
		rte->inh == false ||
		/* row locks not necessary */
		parse->rowMarks != NIL ||
		/* not update and/or delete */
		0 != parse->resultRelation)
		return false;

	return true;
}

/*  get chunk oids specified by explicit chunk exclusion function */
static List *
get_explicit_chunk_oids(CollectQualCtx *ctx, Hypertable *ht)
{
	List *chunk_oids = NIL;
	Const *chunks_arg;
	ArrayIterator chunk_id_iterator;
	Datum elem = (Datum) NULL;
	bool isnull;
	Expr *expr;

	Assert(ctx->chunk_exclusion_func->args->length == 2);
	expr = lsecond(ctx->chunk_exclusion_func->args);
	if (!IsA(expr, Const))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("second argument to chunk_in should contain only integer consts")));

	chunks_arg = (Const *) expr;

	/* function marked as STRICT so argument can't be NULL */
	Assert(!chunks_arg->constisnull);

	chunk_id_iterator = array_create_iterator(DatumGetArrayTypeP(chunks_arg->constvalue), 0, NULL);

	while (array_iterate(chunk_id_iterator, &elem, &isnull))
	{
		if (!isnull)
		{
			int32 chunk_id = DatumGetInt32(elem);
			Chunk *chunk = ts_chunk_get_by_id(chunk_id, 0, false);

			if (chunk == NULL)
				ereport(ERROR, (errmsg("chunk id %d not found", chunk_id)));

			if (chunk->fd.hypertable_id != ht->fd.id)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("chunk id %d does not belong to hypertable \"%s\"",
								chunk_id,
								NameStr(ht->fd.table_name))));

			chunk_oids = lappend_int(chunk_oids, chunk->table_id);
		}
		else
			elog(ERROR, "chunk id can't be NULL");
	}
	array_free_iterator(chunk_id_iterator);
	return chunk_oids;
}

/**
 * Get chunk oids from either restrict info or explicit chunk exclusion. Explicit chunk exclusion
 * takes precedence.
 */
static List *
get_chunk_oids(CollectQualCtx *ctx, PlannerInfo *root, RelOptInfo *rel, Hypertable *ht)
{
	bool reverse;

	if (ctx->chunk_exclusion_func == NULL)
	{
		HypertableRestrictInfo *hri = ts_hypertable_restrict_info_create(rel, ht);

		/*
		 * This is where the magic happens: use our HypertableRestrictInfo
		 * infrastructure to deduce the appropriate chunks using our range
		 * exclusion
		 */
		ts_hypertable_restrict_info_add(hri, root, ctx->restrictions);

		if (should_order_append(root, rel, ht, &reverse))
		{
			if (rel->fdw_private != NULL)
				((TimescaleDBPrivate *) rel->fdw_private)->appends_ordered = true;
			return ts_hypertable_restrict_info_get_chunk_oids_ordered(hri,
																	  ht,
																	  AccessShareLock,
																	  reverse);
		}
		else
			return find_children_oids(hri, ht, AccessShareLock);
	}
	else
		return get_explicit_chunk_oids(ctx, ht);
}

/* Inspired by expand_inherited_rtentry but expands
 * a hypertable chunks into an append relationship */
void
ts_plan_expand_hypertable_chunks(Hypertable *ht, PlannerInfo *root, Oid parent_oid, bool inhparent,
								 RelOptInfo *rel)
{
	RangeTblEntry *rte = rt_fetch(rel->relid, root->parse->rtable);
	List *inh_oids;
	ListCell *l;
	Relation oldrelation = heap_open(parent_oid, NoLock);
	Query *parse = root->parse;
	Index rti = rel->relid;
	List *appinfos = NIL;
	PlanRowMark *oldrc;
	CollectQualCtx ctx = {
		.root = root,
		.rel = rel,
		.restrictions = NIL,
		.chunk_exclusion_func = NULL,
	};

	/* double check our permissions are valid */
	Assert(rti != parse->resultRelation);
	oldrc = get_plan_rowmark(root->rowMarks, rti);
	if (oldrc && RowMarkRequiresRowShareLock(oldrc->markType))
		elog(ERROR, "unexpected permissions requested");

	/* mark the parent as an append relation */
	rte->inh = true;

	init_chunk_exclusion_func();

	/* Walk the tree and find restrictions or chunk exclusion functions */
	collect_quals_mutator((Node *) root->parse->jointree, &ctx);

	inh_oids = get_chunk_oids(&ctx, root, rel, ht);

	/*
	 * the simple_*_array structures have already been set, we need to add the
	 * children to them
	 */
	root->simple_rel_array_size += list_length(inh_oids);
	root->simple_rel_array =
		repalloc(root->simple_rel_array, root->simple_rel_array_size * sizeof(RelOptInfo *));
	root->simple_rte_array =
		repalloc(root->simple_rte_array, root->simple_rel_array_size * sizeof(RangeTblEntry *));

	foreach (l, inh_oids)
	{
		Oid child_oid = lfirst_oid(l);
		Relation newrelation;
		RangeTblEntry *childrte;
		Index child_rtindex;
		AppendRelInfo *appinfo;

		/* Open rel if needed; we already have required locks */
		if (child_oid != parent_oid)
			newrelation = heap_open(child_oid, NoLock);
		else
			newrelation = oldrelation;

		/* chunks cannot be temp tables */
		Assert(!RELATION_IS_OTHER_TEMP(newrelation));

		/*
		 * Build an RTE for the child, and attach to query's rangetable list.
		 * We copy most fields of the parent's RTE, but replace relation OID
		 * and relkind, and set inh = false.  Also, set requiredPerms to zero
		 * since all required permissions checks are done on the original RTE.
		 * Likewise, set the child's securityQuals to empty, because we only
		 * want to apply the parent's RLS conditions regardless of what RLS
		 * properties individual children may have.  (This is an intentional
		 * choice to make inherited RLS work like regular permissions checks.)
		 * The parent securityQuals will be propagated to children along with
		 * other base restriction clauses, so we don't need to do it here.
		 */
		childrte = copyObject(rte);
		childrte->relid = child_oid;
		childrte->relkind = newrelation->rd_rel->relkind;
		childrte->inh = false;
		/* clear the magic bit */
		childrte->ctename = NULL;
		childrte->requiredPerms = 0;
		childrte->securityQuals = NIL;
		parse->rtable = lappend(parse->rtable, childrte);
		child_rtindex = list_length(parse->rtable);
		root->simple_rte_array[child_rtindex] = childrte;
		root->simple_rel_array[child_rtindex] = NULL;

#if !PG96
		Assert(childrte->relkind != RELKIND_PARTITIONED_TABLE);
#endif

		appinfo = makeNode(AppendRelInfo);
		appinfo->parent_relid = rti;
		appinfo->child_relid = child_rtindex;
		appinfo->parent_reltype = oldrelation->rd_rel->reltype;
		appinfo->child_reltype = newrelation->rd_rel->reltype;
		ts_make_inh_translation_list(oldrelation,
									 newrelation,
									 child_rtindex,
									 &appinfo->translated_vars);
		appinfo->parent_reloid = parent_oid;
		appinfos = lappend(appinfos, appinfo);

		/* Close child relations, but keep locks */
		if (child_oid != parent_oid)
			heap_close(newrelation, NoLock);
	}

	heap_close(oldrelation, NoLock);

	root->append_rel_list = list_concat(root->append_rel_list, appinfos);
#if !PG96 && !PG10
	/*
	 * PG11 introduces a separate array to make looking up children faster, see:
	 * https://github.com/postgres/postgres/commit/7d872c91a3f9d49b56117557cdbb0c3d4c620687.
	 */
	setup_append_rel_array(root);
#endif
}
