use crate::frontend::sql::binder::{BoundFromItem, BoundSelect};
use crate::ir::expr::Expr as IRExpr;
use crate::ir::plan::{Join, LogicalPlan, Sort};

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

    let projections: Vec<(IRExpr, String)> = stmt.columns;

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
