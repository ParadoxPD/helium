use crate::execution::context::ExecutionContext;
use crate::execution::eval_expr::eval_expr;
use crate::execution::executor::{Executor, Row};
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
    fn open(&mut self, ctx: &ExecutionContext) {
        self.input.open(ctx);
    }

    fn next(&mut self) -> Option<Row> {
        while let Some(row) = self.input.next() {
            let value = eval_expr(&self.predicate, &row);

            match value {
                Value::Boolean(true) => return Some(row),
                Value::Boolean(false) | Value::Null => {
                    continue;
                }
                other => {
                    panic!("Filter predicate did not evaluate to boolean: {:?}", other);
                }
            }
        }

        None
    }

    fn close(&mut self) {
        self.input.close();
    }
}
