use crate::execution::context::ExecutionContext;
use crate::execution::errors::TableMutationStats;
use crate::execution::eval_expr::eval_expr;
use crate::execution::executor::{ExecResult, Executor, Row};
use crate::ir::expr::Expr;

pub struct ProjectExecutor<'a> {
    input: Box<dyn Executor<'a>>,
    exprs: Vec<Expr>,
}

impl<'a> ProjectExecutor<'a> {
    pub fn new(input: Box<dyn Executor<'a>>, exprs: Vec<Expr>) -> Self {
        Self { input, exprs }
    }
}

impl<'a> Executor<'a> for ProjectExecutor<'a> {
    fn open(&mut self, ctx: &ExecutionContext) -> ExecResult<()> {
        self.input.open(ctx)
    }

    fn next(&mut self, ctx: &ExecutionContext) -> ExecResult<Option<Row>> {
        let row = match self.input.next(ctx)? {
            Some(r) => r,
            None => return Ok(None),
        };

        let mut out = Vec::with_capacity(self.exprs.len());
        for expr in &self.exprs {
            out.push(eval_expr(expr, &row)?);
        }

        Ok(Some(out))
    }

    fn close(&mut self, ctx: &ExecutionContext) -> ExecResult<Vec<TableMutationStats>> {
        self.input.close(ctx)
    }
}
