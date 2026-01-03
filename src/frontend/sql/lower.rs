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

pub fn lower_stmt(stmt: Statement, catalog: &Catalog) -> Lowered {
    match stmt {
        Statement::Explain { analyze, stmt } => {
            let inner = lower_stmt(*stmt, catalog);
            match inner {
                Lowered::Plan(plan) => Lowered::Explain { analyze, plan },
                _ => unreachable!("EXPLAIN can only wrap a plan"),
            }
        }

        // SELECT goes through binder → logical plan
        Statement::Select(_) => {
            let mut binder = Binder::new(catalog);

            match binder.bind_statement(stmt).expect("bind error") {
                BoundStatement::Select(bound) => Lowered::Plan(lower_select(bound)),
                _ => unreachable!("SELECT must bind to BoundSelect"),
            }
        }

        // CREATE INDEX must be validated by binder
        Statement::CreateIndex {
            name,
            table,
            column,
        } => {
            let mut binder = Binder::new(catalog);

            // validate table + column existence
            binder.tables.insert(table.clone(), table.clone());
            binder.resolve_column(&table, &column).expect("bind error");

            Lowered::CreateIndex {
                name,
                table,
                column,
            }
        }

        // DROP INDEX is syntactic for now
        Statement::DropIndex { name } => Lowered::DropIndex { name },

        _ => panic!("statement not lowered here"),
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
