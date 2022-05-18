/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <access/xact.h>
#include <datatype/timestamp.h>
#include <nodes/makefuncs.h>
#include <optimizer/optimizer.h>
#include <utils/fmgroids.h>

#include "cache.h"
#include "dimension.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "planner.h"

/*
 * This implements an optimization to allow now() expression to be
 * used during plan time chunk exclusions. Since now() is stable it
 * would not normally be considered for plan time chunk exclusion.
 * To enable this behaviour we convert `column > now()` expressions
 * into `column > const AND column > now()`. Assuming that times
 * always moves forward this is safe even for prepared statements.
 *
 * We consider the following expressions valid for this optimization:
 * - Var > now()
 * - Var >= now()
 * - Var > now() - Interval
 * - Var > now() + Interval
 * - Var >= now() - Interval
 * - Var >= now() + Interval
 *
 * Additionally Interval needs to be Const and not contain day or month
 * components as those would be affected by timezone, which can change
 * between executions of a prepared statement.
 */
static const Dimension *
get_hypertable_dimension(Oid relid)
{
	Hypertable *ht = ts_planner_get_hypertable(relid, CACHE_FLAG_CHECK);
	if (!ht)
		return NULL;
	return hyperspace_get_open_dimension(ht->space, 0);
}

static bool
is_valid_now_expr(OpExpr *op, List *rtable)
{
	/* Var > or Var >= */
	if ((op->opfuncid != F_TIMESTAMPTZ_GT && op->opfuncid != F_TIMESTAMPTZ_GE) ||
		!IsA(linitial(op->args), Var))
		return false;

	/*
	 * Check that the constraint is actually on a partitioning
	 * column. We only check for match on first open dimension
	 * because that will be the time column.
	 */
	Var *var = linitial_node(Var, op->args);
	Assert(var->varno <= list_length(rtable));
	RangeTblEntry *rte = list_nth(rtable, var->varno - 1);

	const Dimension *dim = get_hypertable_dimension(rte->relid);
	if (!dim || dim->fd.column_type != TIMESTAMPTZOID || dim->column_attno != var->varattno)
		return false;

	/* Var > now() or Var >= now() */
	if (IsA(lsecond(op->args), FuncExpr) && lsecond_node(FuncExpr, op->args)->funcid == F_NOW)
		return true;

	if (!IsA(lsecond(op->args), OpExpr))
		return false;

	/* Var >|>= now() +|- Const */
	OpExpr *op_inner = lsecond_node(OpExpr, op->args);
	if ((op_inner->opfuncid != F_TIMESTAMPTZ_MI_INTERVAL &&
		 op_inner->opfuncid != F_TIMESTAMPTZ_PL_INTERVAL) ||
		!IsA(linitial(op_inner->args), FuncExpr) ||
		linitial_node(FuncExpr, op_inner->args)->funcid != F_NOW ||
		!IsA(lsecond(op_inner->args), Const))
		return false;

	/*
	 * The consttype check should not be necessary since the
	 * operators we whitelist above already mandates it.
	 */
	Const *c = lsecond_node(Const, op_inner->args);
	Assert(c->consttype == INTERVALOID);
	if (c->constisnull || c->consttype != INTERVALOID)
		return false;

	Interval *offset = DatumGetIntervalP(c->constvalue);
	/*
	 * We don't consider day or month intervals safe here as
	 * they are affected by timezones and therefore not
	 * safe to evaluate during planning.
	 */
	if (offset->day != 0 || offset->month != 0)
		return false;

	return true;
}

static Const *
make_now_const()
{
	return makeConst(TIMESTAMPTZOID,
					 -1,
					 InvalidOid,
					 sizeof(TimestampTz),
#ifdef TS_DEBUG
					 ts_get_mock_time_or_current_time(),
#else
					 TimestampTzGetDatum(GetCurrentTransactionStartTimestamp()),
#endif
					 false,
					 FLOAT8PASSBYVAL);
}

/* returns a copy of the expression with the now() call constified */
/*
 * op will be OpExpr with Var > now() - Expr
 */
static OpExpr *
constify_now_expr(PlannerInfo *root, OpExpr *op)
{
	op = copyObject(op);
	if (IsA(lsecond(op->args), FuncExpr))
	{
		/*
		 * Sanity check that this is a supported expression. We should never
		 * end here if it isn't since this is checked in is_valid_now_expr.
		 */
		Assert(lsecond_node(FuncExpr, op->args)->funcid == F_NOW);
		lsecond(op->args) = make_now_const();

		return op;
	}
	else
	{
		OpExpr *op_inner = lsecond_node(OpExpr, op->args);
		/*
		 * Sanity check that this is a supported expression. We should never
		 * end here if it isn't since this is checked in is_valid_now_expr.
		 */
		Assert(linitial_node(FuncExpr, op_inner->args)->funcid == F_NOW);
		linitial(op_inner->args) = make_now_const();

		/*
		 * Normally estimate_expression_value is not safe to use during planning
		 * since it also evaluates stable expressions. Since we only allow a
		 * very limited subset of expressions for this optimization it is safe
		 * for those expressions we allowed earlier.
		 * estimate_expression_value should always be able to completely constify
		 * the expression due to the restrictions we impose on the expressions
		 * supported.
		 */
		lsecond(op->args) = estimate_expression_value(root, (Node *) op_inner);
		Assert(IsA(lsecond(op->args), Const));
		return op;
	}
}

Node *
ts_constify_now(PlannerInfo *root, List *rtable, Node *node)
{
	Assert(node);

	switch (nodeTag(node))
	{
		case T_OpExpr:
			if (is_valid_now_expr(castNode(OpExpr, node), rtable))
			{
				List *args =
					list_make2(copyObject(node), constify_now_expr(root, castNode(OpExpr, node)));
				return (Node *) makeBoolExpr(AND_EXPR, args, -1);
			}
			break;
		case T_BoolExpr:
		{
			List *additions = NIL;
			ListCell *lc;
			BoolExpr *be = castNode(BoolExpr, node);

			/* We only look for top-level AND */
			if (be->boolop != AND_EXPR)
				return node;

			foreach (lc, be->args)
			{
				if (IsA(lfirst(lc), OpExpr) && is_valid_now_expr(lfirst_node(OpExpr, lc), rtable))
				{
					OpExpr *op = lfirst_node(OpExpr, lc);
					additions = lappend(additions, constify_now_expr(root, op));
				}
			}

			if (additions)
			{
				be->args = list_concat(be->args, additions);
			}

			break;
		}
		default:
			break;
	}

	return node;
}
