use crate::execution::context::ExecutionContext;
use crate::execution::eval_expr::eval_expr;
use crate::execution::executor::{Executor, Row};
use crate::ir::expr::Expr;

pub struct ProjectExecutor {
    input: Box<dyn Executor>,
    exprs: Vec<Expr>,
}

impl ProjectExecutor {
    pub fn new(input: Box<dyn Executor>, exprs: Vec<Expr>) -> Self {
        Self { input, exprs }
    }
}

impl Executor for ProjectExecutor {
    fn open(&mut self, ctx: &ExecutionContext) {
        self.input.open(ctx);
    }

    fn next(&mut self) -> Option<Row> {
        let row = self.input.next()?;

        let mut out = Vec::with_capacity(self.exprs.len());
        for expr in &self.exprs {
            let v = eval_expr(expr, &row);
            out.push(v);
        }

        Some(out)
    }

    fn close(&mut self) {
        self.input.close();
    }
}
