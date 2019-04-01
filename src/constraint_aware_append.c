/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_cast.h>
#include <catalog/pg_class.h>
#include <catalog/pg_namespace.h>
#include <catalog/pg_operator.h>
#include <commands/explain.h>
#include <executor/executor.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/nodes.h>
#include <nodes/plannodes.h>
#include <optimizer/clauses.h>
#include <optimizer/plancat.h>
#include <optimizer/prep.h>
#include <parser/parsetree.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/syscache.h>

#include "constraint_aware_append.h"
#include "hypertable.h"
#include "compat.h"

/*
 * Exclude child relations (chunks) at execution time based on constraints.
 *
 * This functions tries to reuse as much functionality as possible from standard
 * constraint exclusion in PostgreSQL that normally happens at planning
 * time. Therefore, we need to fake a number of planning-related data
 * structures.
 */
static bool
excluded_by_constraint(PlannerInfo *root, RangeTblEntry *rte, Index rt_index, List *restrictinfos)
{
	RelOptInfo rel = {
		.type = T_RelOptInfo,
		.relid = rt_index,
		.reloptkind = RELOPT_OTHER_MEMBER_REL,
		.baserestrictinfo = restrictinfos,
	};

	return relation_excluded_by_constraints(root, &rel, rte);
}

static Plan *
get_plans_for_exclusion(Plan *plan)
{
	/* Optimization: If we want to be able to prune */
	/* when the node is a T_Result, then we need to peek */
	/* into the subplans of this Result node. */
	if (IsA(plan, Result))
	{
		Result *res = (Result *) plan;

		if (res->plan.lefttree != NULL && res->plan.righttree == NULL)
			return res->plan.lefttree;
		if (res->plan.righttree != NULL && res->plan.lefttree == NULL)
			return res->plan.righttree;
	}
	return plan;
}

static AppendRelInfo *
get_appendrelinfo(PlannerInfo *root, Index rti)
{
#if PG96 || PG10
	ListCell *lc;
	foreach (lc, root->append_rel_list)
	{
		AppendRelInfo *appinfo = lfirst(lc);
		if (appinfo->child_relid == rti)
			return appinfo;
	}
#else
	if (root->append_rel_array[rti])
		return root->append_rel_array[rti];
#endif
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR), errmsg("no appendrelinfo found for index %d", rti)));
	pg_unreachable();
}

static bool
can_exclude_chunk(PlannerInfo *root, Scan *scan, EState *estate, Index rt_index,
				  List *restrictinfos)
{
	RangeTblEntry *rte = rt_fetch(scan->scanrelid, estate->es_range_table);

	return rte->rtekind == RTE_RELATION && rte->relkind == RELKIND_RELATION && !rte->inh &&
		   excluded_by_constraint(root, rte, rt_index, restrictinfos);
}

/*
 * get_operator
 *
 *    finds an operator given an exact specification (name, namespace,
 *    left and right type IDs).
 */
static Oid
get_operator(const char *name, Oid namespace, Oid left, Oid right)
{
	HeapTuple tup;
	Oid opoid = InvalidOid;

	tup = SearchSysCache4(OPERNAMENSP,
						  PointerGetDatum(name),
						  ObjectIdGetDatum(left),
						  ObjectIdGetDatum(right),
						  ObjectIdGetDatum(namespace));
	if (HeapTupleIsValid(tup))
	{
		opoid = HeapTupleGetOid(tup);
		ReleaseSysCache(tup);
	}

	return opoid;
}

/*
 * lookup cast func oid in pg_cast
 */
static Oid
get_cast_func(Oid source, Oid target)
{
	Oid result = InvalidOid;
	HeapTuple casttup;

	casttup = SearchSysCache2(CASTSOURCETARGET, ObjectIdGetDatum(source), ObjectIdGetDatum(target));
	if (HeapTupleIsValid(casttup))
	{
		Form_pg_cast castform = (Form_pg_cast) GETSTRUCT(casttup);

		result = castform->castfunc;
		ReleaseSysCache(casttup);
	}

	return result;
}

#define DATATYPE_PAIR(left, right, type1, type2)                                                   \
	((left == type1 && right == type2) || (left == type2 && right == type1))

/*
 * Cross datatype comparisons between DATE/TIMESTAMP/TIMESTAMPTZ
 * are not immutable which prevents their usage for chunk exclusion.
 * Unfortunately estimate_expression_value will not estimate those
 * expressions which makes them unusable for execution time chunk
 * exclusion with constraint aware append.
 * To circumvent this we inject casts and use an operator
 * with the same datatype on both sides when constifying
 * restrictinfo. This allows estimate_expression_value
 * to evaluate those expressions and makes them accessible for
 * execution time chunk exclusion.
 *
 * The following transformations are done:
 * TIMESTAMP OP TIMESTAMPTZ => TIMESTAMP OP (TIMESTAMPTZ::TIMESTAMP)
 * TIMESTAMPTZ OP DATE => TIMESTAMPTZ OP (DATE::TIMESTAMPTZ)
 *
 * No transformation is required for TIMESTAMP OP DATE because
 * those operators are marked immutable.
 */
static Expr *
transform_restrict_info_clause(Expr *clause)
{
	clause = copyObject(clause);
	if (IsA(clause, OpExpr) && list_length(castNode(OpExpr, clause)->args) == 2)
	{
		OpExpr *op = castNode(OpExpr, clause);
		Oid left_type = exprType(linitial(op->args));
		Oid right_type = exprType(lsecond(op->args));

		if (op->opresulttype != BOOLOID || op->opretset == true)
			return clause;

		if (!IsA(linitial(op->args), Var) && !IsA(lsecond(op->args), Var))
			return clause;

		if (DATATYPE_PAIR(left_type, right_type, TIMESTAMPOID, TIMESTAMPTZOID) ||
			DATATYPE_PAIR(left_type, right_type, TIMESTAMPTZOID, DATEOID))
		{
			char *opname = get_opname(op->opno);
			Oid source_type, target_type, opno, cast_oid;

			/*
			 * if Var is on left side we put cast on right side otherwise
			 * it will be left
			 */
			if (IsA(linitial(op->args), Var))
			{
				source_type = right_type;
				target_type = left_type;
			}
			else
			{
				source_type = left_type;
				target_type = right_type;
			}

			opno = get_operator(opname, PG_CATALOG_NAMESPACE, target_type, target_type);
			cast_oid = get_cast_func(source_type, target_type);

			if (OidIsValid(opno) && OidIsValid(cast_oid))
			{
				Expr *left = linitial(op->args);
				Expr *right = lsecond(op->args);

				if (source_type == left_type)
					left = (Expr *) makeFuncExpr(cast_oid,
												 target_type,
												 list_make1(left),
												 InvalidOid,
												 InvalidOid,
												 0);
				else
					right = (Expr *) makeFuncExpr(cast_oid,
												  target_type,
												  list_make1(right),
												  InvalidOid,
												  InvalidOid,
												  0);

				clause = make_opclause(opno, BOOLOID, false, left, right, InvalidOid, InvalidOid);
			}
		}
	}
	return clause;
}

/*
 * Convert restriction clauses to constants expressions (i.e., if there are
 * mutable functions, they need to be evaluated to constants).  For instance,
 * something like:
 *
 * ...WHERE time > now - interval '1 hour'
 *
 * becomes
 *
 * ...WHERE time > '2017-06-02 11:26:43.935712+02'
 */
static List *
constify_restrictinfos(PlannerInfo *root, List *restrictinfos)
{
	ListCell *lc;

	foreach (lc, restrictinfos)
	{
		RestrictInfo *rinfo = lfirst(lc);

		rinfo->clause = (Expr *) estimate_expression_value(root, (Node *) rinfo->clause);
	}

	return restrictinfos;
}

/*
 * Initialize the scan state and prune any subplans from the Append node below
 * us in the plan tree. Pruning happens by evaluating the subplan's table
 * constraints against a folded version of the restriction clauses in the query.
 */
static void
ca_append_begin(CustomScanState *node, EState *estate, int eflags)
{
	ConstraintAwareAppendState *state = (ConstraintAwareAppendState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	Plan *subplan = copyObject(state->subplan);
	List *chunk_ri_clauses = lsecond(cscan->custom_private);
	List **appendplans, *old_appendplans;
	ListCell *lc_plan;
	ListCell *lc_clauses;

	/*
	 * create skeleton plannerinfo to reuse some PostgreSQL planner functions
	 */
	Query parse = {
		.resultRelation = InvalidOid,
	};
	PlannerGlobal glob = {
		.boundParams = NULL,
	};
	PlannerInfo root = {
		.glob = &glob,
		.parse = &parse,
	};

	switch (nodeTag(subplan))
	{
		case T_Append:
		{
			Append *append = (Append *) subplan;

			old_appendplans = append->appendplans;
			append->appendplans = NIL;
			appendplans = &append->appendplans;
			break;
		}
		case T_MergeAppend:
		{
			MergeAppend *append = (MergeAppend *) subplan;

			old_appendplans = append->mergeplans;
			append->mergeplans = NIL;
			appendplans = &append->mergeplans;
			break;
		}
		case T_Result:

			/*
			 * Append plans are turned into a Result node if empty. This can
			 * happen if children are pruned first by constraint exclusion
			 * while we also remove the main table from the appendplans list,
			 * leaving an empty list. In that case, there is nothing to do.
			 */
			return;
		default:
			elog(ERROR, "invalid child of constraint-aware append: %u", nodeTag(subplan));
	}

	/*
	 * clauses should always have the same length as appendplans because
	 * thats the base for building the lists
	 */
	Assert(list_length(old_appendplans) == list_length(chunk_ri_clauses));

	forboth (lc_plan, old_appendplans, lc_clauses, chunk_ri_clauses)
	{
		Plan *plan = get_plans_for_exclusion(lfirst(lc_plan));

		switch (nodeTag(plan))
		{
			case T_SeqScan:
			case T_SampleScan:
			case T_IndexScan:
			case T_IndexOnlyScan:
			case T_BitmapIndexScan:
			case T_BitmapHeapScan:
			case T_TidScan:
			case T_SubqueryScan:
			case T_FunctionScan:
			case T_ValuesScan:
			case T_CteScan:
			case T_WorkTableScan:
			case T_ForeignScan:
			case T_CustomScan:
			{
				/*
				 * If this is a base rel (chunk), check if it can be
				 * excluded from the scan. Otherwise, fall through.
				 */

				Index scanrelid = ((Scan *) plan)->scanrelid;
				List *restrictinfos = NIL;
				List *ri_clauses = lfirst(lc_clauses);
				ListCell *lc;

				Assert(scanrelid);

				foreach (lc, ri_clauses)
				{
					RestrictInfo *ri = makeNode(RestrictInfo);
					ri->clause = lfirst(lc);
					restrictinfos = lappend(restrictinfos, ri);
				}
				restrictinfos = constify_restrictinfos(&root, restrictinfos);

				if (can_exclude_chunk(&root, (Scan *) plan, estate, scanrelid, restrictinfos))
					continue;

				*appendplans = lappend(*appendplans, plan);
				break;
			}
			default:
				elog(ERROR, "invalid child of constraint-aware append: %u", nodeTag(plan));
				break;
		}
	}

	state->num_append_subplans = list_length(*appendplans);
	if (state->num_append_subplans > 0)
		node->custom_ps = list_make1(ExecInitNode(subplan, estate, eflags));
}

static TupleTableSlot *
ca_append_exec(CustomScanState *node)
{
	ConstraintAwareAppendState *state = (ConstraintAwareAppendState *) node;
	TupleTableSlot *subslot;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
#if PG96
	TupleTableSlot *resultslot;
	ExprDoneCond isDone;
#endif

	/*
	 * Check if all append subplans were pruned. In that case there is nothing
	 * to do.
	 */
	if (state->num_append_subplans == 0)
		return NULL;

#if PG96
	if (node->ss.ps.ps_TupFromTlist)
	{
		resultslot = ExecProject(node->ss.ps.ps_ProjInfo, &isDone);

		if (isDone == ExprMultipleResult)
			return resultslot;

		node->ss.ps.ps_TupFromTlist = false;
	}
#endif

	ResetExprContext(econtext);

	while (true)
	{
		subslot = ExecProcNode(linitial(node->custom_ps));

		if (TupIsNull(subslot))
			return NULL;

		if (!node->ss.ps.ps_ProjInfo)
			return subslot;

		econtext->ecxt_scantuple = subslot;

#if PG96
		resultslot = ExecProject(node->ss.ps.ps_ProjInfo, &isDone);

		if (isDone != ExprEndResult)
		{
			node->ss.ps.ps_TupFromTlist = (isDone == ExprMultipleResult);
			return resultslot;
		}
#else
		return ExecProject(node->ss.ps.ps_ProjInfo);
#endif
	}
}

static void
ca_append_end(CustomScanState *node)
{
	if (node->custom_ps != NIL)
	{
		ExecEndNode(linitial(node->custom_ps));
	}
}

static void
ca_append_rescan(CustomScanState *node)
{
#if PG96
	node->ss.ps.ps_TupFromTlist = false;
#endif
	if (node->custom_ps != NIL)
	{
		ExecReScan(linitial(node->custom_ps));
	}
}

static void
ca_append_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	ConstraintAwareAppendState *state = (ConstraintAwareAppendState *) node;
	Oid relid = linitial_oid(linitial(cscan->custom_private));

	ExplainPropertyText("Hypertable", get_rel_name(relid), es);
#if PG96 || PG10
	ExplainPropertyInteger("Chunks left after exclusion", state->num_append_subplans, es);
#else

	/*
	 * PG 11 adds a uint field for certain cases. (See:
	 * https://github.com/postgres/postgres/commit/7a50bb690b4837d29e715293c156cff2fc72885c).
	 */
	ExplainPropertyInteger("Chunks left after exclusion", NULL, state->num_append_subplans, es);
#endif
}

static CustomExecMethods constraint_aware_append_state_methods = {
	.BeginCustomScan = ca_append_begin,
	.ExecCustomScan = ca_append_exec,
	.EndCustomScan = ca_append_end,
	.ReScanCustomScan = ca_append_rescan,
	.ExplainCustomScan = ca_append_explain,
};

static Node *
constraint_aware_append_state_create(CustomScan *cscan)
{
	ConstraintAwareAppendState *state;
	Append *append = linitial(cscan->custom_plans);

	state = (ConstraintAwareAppendState *) newNode(sizeof(ConstraintAwareAppendState),
												   T_CustomScanState);
	state->csstate.methods = &constraint_aware_append_state_methods;
	state->subplan = &append->plan;

	return (Node *) state;
}

static CustomScanMethods constraint_aware_append_plan_methods = {
	.CustomName = "ConstraintAwareAppend",
	.CreateCustomScanState = constraint_aware_append_state_create,
};

static Plan *
constraint_aware_append_plan_create(PlannerInfo *root, RelOptInfo *rel, struct CustomPath *path,
									List *tlist, List *clauses, List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);
	Plan *subplan = linitial(custom_plans);
	RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);
	List *chunk_ri_clauses = NIL;
	List *children = NIL;
	ListCell *lc_child;

	cscan->scan.scanrelid = 0;			 /* Not a real relation we are scanning */
	cscan->scan.plan.targetlist = tlist; /* Target list we expect as output */
	cscan->custom_plans = custom_plans;

	/*
	 * create per chunk RestrictInfo
	 *
	 * We also need to walk the expression trees of the restriction clauses and
	 * update any Vars that reference the main table to instead reference the child
	 * table (chunk) we want to exclude.
	 */
	switch (nodeTag(linitial(custom_plans)))
	{
		case T_MergeAppend:
			children = castNode(MergeAppend, linitial(custom_plans))->mergeplans;
			break;
		case T_Append:
			children = castNode(Append, linitial(custom_plans))->appendplans;
			break;
		default:
			elog(ERROR,
				 "invalid child of constraint-aware append: %u",
				 nodeTag(linitial(custom_plans)));
			break;
	}

	/*
	 * we only iterate over the child chunks of this node
	 * so the list of metadata exactly matches the list of
	 * child nodes in the executor
	 */
	foreach (lc_child, children)
	{
		Plan *plan = get_plans_for_exclusion(lfirst(lc_child));

		switch (nodeTag(plan))
		{
			case T_SeqScan:
			case T_SampleScan:
			case T_IndexScan:
			case T_IndexOnlyScan:
			case T_BitmapIndexScan:
			case T_BitmapHeapScan:
			case T_TidScan:
			case T_SubqueryScan:
			case T_FunctionScan:
			case T_ValuesScan:
			case T_CteScan:
			case T_WorkTableScan:
			case T_ForeignScan:
			case T_CustomScan:
			{
				List *chunk_clauses = NIL;
				ListCell *lc;
				Index scanrelid = ((Scan *) plan)->scanrelid;
				AppendRelInfo *appinfo = get_appendrelinfo(root, scanrelid);

				foreach (lc, clauses)
				{
					Node *clause = (Node *) transform_restrict_info_clause(
						castNode(RestrictInfo, lfirst(lc))->clause);
					clause = adjust_appendrel_attrs_compat(root, clause, appinfo);
					chunk_clauses = lappend(chunk_clauses, clause);
				}
				chunk_ri_clauses = lappend(chunk_ri_clauses, chunk_clauses);
				break;
			}
			default:
				elog(ERROR, "invalid child of constraint-aware append: %u", nodeTag(plan));
				break;
		}
	}

	cscan->custom_private = list_make2(list_make1_oid(rte->relid), chunk_ri_clauses);
	cscan->custom_scan_tlist = subplan->targetlist; /* Target list of tuples
													 * we expect as input */
	cscan->flags = path->flags;
	cscan->methods = &constraint_aware_append_plan_methods;

	return &cscan->scan.plan;
}

static CustomPathMethods constraint_aware_append_path_methods = {
	.CustomName = "ConstraintAwareAppend",
	.PlanCustomPath = constraint_aware_append_plan_create,
};

Path *
ts_constraint_aware_append_path_create(PlannerInfo *root, Hypertable *ht, Path *subpath)
{
	ConstraintAwareAppendPath *path;

	path = (ConstraintAwareAppendPath *) newNode(sizeof(ConstraintAwareAppendPath), T_CustomPath);
	path->cpath.path.pathtype = T_CustomScan;
	path->cpath.path.rows = subpath->rows;
	path->cpath.path.startup_cost = subpath->startup_cost;
	path->cpath.path.total_cost = subpath->total_cost;
	path->cpath.path.parent = subpath->parent;
	path->cpath.path.pathkeys = subpath->pathkeys;
	path->cpath.path.param_info = subpath->param_info;
	path->cpath.path.pathtarget = subpath->pathtarget;

	path->cpath.path.parallel_aware = false;
	path->cpath.path.parallel_safe = subpath->parallel_safe;
	path->cpath.path.parallel_workers = subpath->parallel_workers;

	/*
	 * Set flags. We can set CUSTOMPATH_SUPPORT_BACKWARD_SCAN and
	 * CUSTOMPATH_SUPPORT_MARK_RESTORE. The only interesting flag is the first
	 * one (backward scan), but since we are not scanning a real relation we
	 * need not indicate that we support backward scans. Lower-level index
	 * scanning nodes will scan backward if necessary, so once tuples get to
	 * this node they will be in a given order already.
	 */
	path->cpath.flags = 0;
	path->cpath.custom_paths = list_make1(subpath);
	path->cpath.methods = &constraint_aware_append_path_methods;

	/*
	 * Make sure our subpath is either an Append or MergeAppend node
	 */
	switch (nodeTag(subpath))
	{
		case T_AppendPath:
		case T_MergeAppendPath:
			break;
		default:
			elog(ERROR, "invalid child of constraint-aware append: %u", nodeTag(subpath));
			break;
	}

	return &path->cpath.path;
}

void
_constraint_aware_append_init(void)
{
	RegisterCustomScanMethods(&constraint_aware_append_plan_methods);
}
