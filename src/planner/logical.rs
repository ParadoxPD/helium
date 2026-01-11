//! Logical planner.
//!
//! Lowers bound statements into frozen logical IR.

use crate::binder::bound::BoundExpr;
use crate::binder::bound::*;
use crate::catalog::ids::ColumnId;
use crate::ir::expr::Expr;
use crate::ir::plan::{JoinType, LogicalPlan, SortKey};
use crate::planner::errors::{PlanError, PlanResult};

pub struct LogicalPlanner;

impl LogicalPlanner {
    pub fn new() -> Self {
        Self
    }

    pub fn plan(&self, stmt: BoundStatement) -> Result<LogicalPlan, PlanError> {
        match stmt {
            BoundStatement::Select(s) => self.plan_select(s),

            BoundStatement::Insert(s) => Ok(self.plan_insert(s)),

            BoundStatement::Update(s) => Ok(self.plan_update(s)),

            BoundStatement::Delete(s) => Ok(self.plan_delete(s)),

            BoundStatement::Explain { stmt, .. } => self.plan(*stmt),

            BoundStatement::CreateTable(_)
            | BoundStatement::DropTable(_)
            | BoundStatement::CreateIndex(_)
            | BoundStatement::DropIndex(_) => Err(PlanError::InvalidPlan {
                reason: "DDL must bypass logical planner",
            }),
        }
    }
}
impl LogicalPlanner {
    fn plan_select(&self, stmt: BoundSelect) -> Result<LogicalPlan, PlanError> {
        // FROM
        let mut plan = self.plan_from(stmt.from)?;

        // WHERE
        if let Some(predicate) = stmt.selection {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: self.lower_expr(predicate),
            };
        }

        // PROJECT
        if stmt.projection.is_empty() {
            return Err(PlanError::InvalidPlan {
                reason: "SELECT projection cannot be empty",
            });
        }

        plan = LogicalPlan::Project {
            input: Box::new(plan),
            exprs: stmt
                .projection
                .into_iter()
                .map(|e| self.lower_expr(e))
                .collect(),
        };

        // ORDER BY
        if !stmt.order_by.is_empty() {
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                keys: stmt
                    .order_by
                    .into_iter()
                    .map(|(e, asc)| SortKey {
                        expr: self.lower_expr(e),
                        asc,
                    })
                    .collect(),
            };
        }

        // LIMIT / OFFSET
        if let Some(limit) = stmt.limit {
            if limit == 0 {
                return Err(PlanError::InvalidPlan {
                    reason: "LIMIT must be > 0",
                });
            }

            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                limit,
                offset: stmt.offset.unwrap_or(0),
            };
        }

        Ok(plan)
    }
}

impl LogicalPlanner {
    fn plan_from(&self, from: BoundFrom) -> Result<LogicalPlan, PlanError> {
        match from {
            BoundFrom::Table { table_id } => Ok(LogicalPlan::Scan { table_id }),

            BoundFrom::Join {
                left,
                right,
                on,
                join_type,
            } => {
                if matches!(join_type, JoinType::Full) {
                    return Err(PlanError::UnsupportedFeature {
                        feature: "FULL OUTER JOIN",
                    });
                }

                Ok(LogicalPlan::Join {
                    left: Box::new(self.plan_from(*left)?),
                    right: Box::new(self.plan_from(*right)?),
                    on: self.lower_expr(on),
                    join_type,
                })
            }
        }
    }
}

impl LogicalPlanner {
    fn plan_insert(&self, stmt: BoundInsert) -> LogicalPlan {
        LogicalPlan::Insert {
            table_id: stmt.table_id,
            rows: stmt
                .rows
                .into_iter()
                .map(|row| row.into_iter().map(|e| self.lower_expr(e)).collect())
                .collect(),
        }
    }
}

impl LogicalPlanner {
    fn plan_update(&self, stmt: BoundUpdate) -> LogicalPlan {
        LogicalPlan::Update {
            table_id: stmt.table_id,
            assignments: stmt
                .assignments
                .into_iter()
                .map(|(col_id, expr)| (col_id, self.lower_expr(expr)))
                .collect(),
            predicate: stmt.predicate.map(|p| self.lower_expr(p)),
        }
    }
}

impl LogicalPlanner {
    fn plan_delete(&self, stmt: BoundDelete) -> LogicalPlan {
        LogicalPlan::Delete {
            table_id: stmt.table_id,
            predicate: stmt.predicate.map(|p| self.lower_expr(p)),
        }
    }
}

impl LogicalPlanner {
    fn lower_expr(&self, expr: BoundExpr) -> Expr {
        match expr {
            BoundExpr::Column { column_id } => Expr::BoundColumn { column_id },

            BoundExpr::Literal(v) => Expr::Literal(v),

            BoundExpr::Unary { op, expr } => Expr::Unary {
                op,
                expr: Box::new(self.lower_expr(*expr)),
            },

            BoundExpr::Binary { left, op, right } => Expr::Binary {
                left: Box::new(self.lower_expr(*left)),
                op,
                right: Box::new(self.lower_expr(*right)),
            },

            BoundExpr::Null => Expr::Null,
        }
    }
}
