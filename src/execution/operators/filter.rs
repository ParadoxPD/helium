use crate::execution::context::ExecutionContext;
use crate::execution::errors::{ExecutionError, TableMutationStats};
use crate::execution::eval_expr::eval_expr;
use crate::execution::executor::{ExecResult, Executor, Row};
use crate::ir::expr::Expr;
use crate::types::value::Value;

pub struct FilterExecutor {
    input: Box<dyn Executor>,
    predicate: Expr,
}

impl FilterExecutor {
    pub fn new(input: Box<dyn Executor>, predicate: Expr) -> Self {
        Self { input, predicate }
    }
}

impl Executor for FilterExecutor {
    fn open(&mut self, ctx: &mut ExecutionContext) -> ExecResult<()> {
        self.input.open(ctx)
    }

    fn next(&mut self, ctx: &mut ExecutionContext) -> ExecResult<Option<Row>> {
        while let Some(row) = self.input.next(ctx)? {
            match eval_expr(&self.predicate, &row)? {
                Value::Boolean(true) => return Ok(Some(row)),
                Value::Boolean(false) | Value::Null => {
                    ctx.stats.rows_filtered += 1;
                    continue;
                }
                _ => {
                    return Err(ExecutionError::InvalidExpression {
                        reason: "filter predicate must be boolean".into(),
                    });
                }
            }
        }
        Ok(None)
    }

    fn close(&mut self, ctx: &mut ExecutionContext) -> ExecResult<Vec<TableMutationStats>> {
        self.input.close(ctx)
    }
}
