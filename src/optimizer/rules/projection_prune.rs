use std::collections::HashSet;

use crate::{
    catalog::ids::ColumnId,
    ir::{
        expr::Expr,
        plan::{LogicalPlan, SortKey},
    },
    optimizer::errors::OptimizerError,
};

pub fn projection_prune(plan: &LogicalPlan) -> Result<LogicalPlan, OptimizerError> {
    let mut required = HashSet::new();
    collect_required_columns(plan, &mut required);
    Ok(rewrite(plan, &required))
}

pub fn rewrite(plan: &LogicalPlan, required: &HashSet<ColumnId>) -> LogicalPlan {
    match plan {
        // -------------------------
        // LIMIT
        // -------------------------
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => LogicalPlan::Limit {
            input: Box::new(rewrite(input, required)),
            limit: *limit,
            offset: *offset,
        },

        // -------------------------
        // PROJECT
        // -------------------------
        LogicalPlan::Project { input, exprs } => {
            // Keep only expressions that reference required columns
            let kept_exprs: Vec<Expr> = exprs
                .iter()
                .filter(|expr| expr_uses_any(expr, required))
                .cloned()
                .collect();

            // If projection becomes identity, remove it
            if kept_exprs.is_empty() {
                return rewrite(input, required);
            }

            LogicalPlan::Project {
                input: Box::new(rewrite(input, required)),
                exprs: kept_exprs,
            }
        }

        // -------------------------
        // FILTER
        // -------------------------
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(rewrite(input, required)),
            predicate: predicate.clone(),
        },

        // -------------------------
        // SORT
        // -------------------------
        LogicalPlan::Sort { input, keys } => LogicalPlan::Sort {
            input: Box::new(rewrite(input, required)),
            keys: keys.clone(),
        },

        // -------------------------
        // JOIN
        // -------------------------
        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
        } => LogicalPlan::Join {
            left: Box::new(rewrite(left, required)),
            right: Box::new(rewrite(right, required)),
            on: on.clone(),
            join_type: join_type.clone(),
        },

        // -------------------------
        // SCAN / INDEXSCAN (terminal)
        // -------------------------
        LogicalPlan::Scan { .. }
        | LogicalPlan::IndexScan { .. }
        | LogicalPlan::Insert { .. }
        | LogicalPlan::Update { .. }
        | LogicalPlan::Delete { .. } => plan.clone(),
    }
}
fn expr_uses_any(expr: &Expr, required: &HashSet<ColumnId>) -> bool {
    match expr {
        Expr::BoundColumn { column_id, .. } => required.contains(column_id),

        Expr::Unary { expr, .. } => expr_uses_any(expr, required),

        Expr::Binary { left, right, .. } => {
            expr_uses_any(left, required) || expr_uses_any(right, required)
        }

        Expr::Literal(_) | Expr::Null => false,
    }
}

fn collect_columns(expr: &Expr, out: &mut HashSet<ColumnId>) {
    match expr {
        Expr::BoundColumn { column_id, .. } => {
            out.insert(*column_id);
        }
        Expr::Unary { expr, .. } => collect_columns(expr, out),
        Expr::Binary { left, right, .. } => {
            collect_columns(left, out);
            collect_columns(right, out);
        }
        _ => {}
    }
}

fn collect_expr_columns(expr: &Expr, required: &mut HashSet<ColumnId>) {
    match expr {
        Expr::BoundColumn { column_id, .. } => {
            required.insert(*column_id);
        }

        Expr::Unary { expr, .. } => {
            collect_expr_columns(expr, required);
        }

        Expr::Binary { left, right, .. } => {
            collect_expr_columns(left, required);
            collect_expr_columns(right, required);
        }

        Expr::Literal(_) | Expr::Null => {}
    }
}

pub fn collect_required_columns(plan: &LogicalPlan, required: &mut HashSet<ColumnId>) {
    match plan {
        // -------------------------
        // PROJECT
        // -------------------------
        LogicalPlan::Project { input, exprs } => {
            for expr in exprs {
                collect_expr_columns(expr, required);
            }
            collect_required_columns(input, required);
        }

        // -------------------------
        // FILTER
        // -------------------------
        LogicalPlan::Filter { input, predicate } => {
            collect_expr_columns(predicate, required);
            collect_required_columns(input, required);
        }

        // -------------------------
        // SORT
        // -------------------------
        LogicalPlan::Sort { input, keys } => {
            for SortKey { expr, .. } in keys {
                collect_expr_columns(expr, required);
            }
            collect_required_columns(input, required);
        }

        // -------------------------
        // JOIN
        // -------------------------
        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
        } => {
            collect_expr_columns(on, required);
            collect_required_columns(left, required);
            collect_required_columns(right, required);
        }

        // -------------------------
        // LIMIT
        // -------------------------
        LogicalPlan::Limit { input, .. } => {
            collect_required_columns(input, required);
        }

        // -------------------------
        // INDEX SCAN
        // -------------------------
        LogicalPlan::IndexScan { predicate, .. } => {
            use crate::ir::index_predicate::IndexPredicate;

            match predicate {
                IndexPredicate::Eq(v) => {
                    // literal only → no column usage
                    let _ = v;
                }
                IndexPredicate::Range { low, high } => {
                    let _ = (low, high);
                }
            }
        }

        // -------------------------
        // TERMINALS
        // -------------------------
        LogicalPlan::Scan { .. }
        | LogicalPlan::Insert { .. }
        | LogicalPlan::Update { .. }
        | LogicalPlan::Delete { .. } => {}
    }
}
