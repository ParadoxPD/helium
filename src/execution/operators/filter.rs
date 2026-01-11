use crate::execution::context::ExecutionContext;
use crate::execution::errors::{ExecutionError, TableMutationStats};
use crate::execution::eval_expr::eval_expr;
use crate::execution::executor::{ExecResult, Executor, Row};
use crate::ir::expr::Expr;
use crate::types::value::Value;

pub struct FilterExecutor<'a> {
    input: Box<dyn Executor<'a>>,
    predicate: Expr,
}

impl<'a> FilterExecutor<'a> {
    pub fn new(input: Box<dyn Executor<'a>>, predicate: Expr) -> Self {
        Self { input, predicate }
    }
}

impl<'a> Executor<'a> for FilterExecutor<'a> {
    fn open(&mut self, ctx: &ExecutionContext) -> ExecResult<()> {
        self.input.open(ctx)
    }

    fn next(&mut self, ctx: &ExecutionContext) -> ExecResult<Option<Row>> {
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

    fn close(&mut self, ctx: &ExecutionContext) -> ExecResult<Vec<TableMutationStats>> {
        self.input.close(ctx)
    }
}
