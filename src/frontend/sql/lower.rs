use crate::exec::catalog::Catalog;
use crate::frontend::sql::binder::{Binder, BoundFromItem, BoundSelect, BoundStatement};
use crate::frontend::sql::pretty_binder::pretty_bound;
use crate::ir::expr::Expr as IRExpr;
use crate::ir::plan::{Join, LogicalPlan, Sort};

use super::ast::*;

pub enum Lowered {
    Plan(LogicalPlan),
    Explain {
        analyze: bool,
        plan: LogicalPlan,
    },
    CreateIndex {
        name: String,
        table: String,
        column: String,
    },
    DropIndex {
        name: String,
    },
}

pub fn lower_stmt(bound: BoundStatement) -> Lowered {
    match bound {
        BoundStatement::Select(s) => Lowered::Plan(lower_select(s)),

        BoundStatement::CreateIndex(ci) => Lowered::CreateIndex {
            name: ci.name,
            table: ci.table,
            column: ci.column,
        },

        BoundStatement::DropIndex(di) => Lowered::DropIndex { name: di.name },

        _ => panic!("statement should not reach lowering"),
    }
}

pub fn lower_select(stmt: BoundSelect) -> LogicalPlan {
    let mut plan = lower_from(stmt.from);

    if let Some(expr) = stmt.where_clause {
        plan = plan.filter(expr);
    }

    if !stmt.order_by.is_empty() {
        let keys = stmt.order_by;

        plan = LogicalPlan::Sort(Sort {
            input: Box::new(plan),
            keys,
        });
    }

    let projections: Vec<(IRExpr, String)> = stmt
        .columns
        .into_iter()
        .map(|expr| {
            let name = output_name(&expr);
            (expr, name)
        })
        .collect();

    plan = plan.project(projections);

    if let Some(limit) = stmt.limit {
        plan = plan.limit(limit);
    }

    plan
}

fn lower_from(from: BoundFromItem) -> LogicalPlan {
    match from {
        BoundFromItem::Table { name, alias } => LogicalPlan::scan(&name, &alias),

        BoundFromItem::Join { left, right, on } => LogicalPlan::Join(Join {
            left: Box::new(lower_from(*left)),
            right: Box::new(lower_from(*right)),
            on,
        }),
    }
}

fn output_name(expr: &IRExpr) -> String {
    match expr {
        IRExpr::BoundColumn { name, .. } => name.clone(),
        _ => "expr".to_string(),
    }
}
