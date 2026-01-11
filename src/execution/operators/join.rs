use crate::execution::context::ExecutionContext;
use crate::execution::errors::{ExecutionError, TableMutationStats};
use crate::execution::eval_expr::eval_expr;
use crate::execution::executor::{ExecResult, Executor, Row};
use crate::ir::expr::Expr;
use crate::ir::plan::JoinType;
use crate::types::value::Value;

pub struct JoinExecutor<'a> {
    left: Box<dyn Executor<'a>>,
    right: Box<dyn Executor<'a>>,
    on: Expr,
    join_type: JoinType,

    right_buf: Vec<Row>,
    left_row: Option<Row>,
    right_pos: usize,
}

impl<'a> JoinExecutor<'a> {
    pub fn new(
        left: Box<dyn Executor<'a>>,
        right: Box<dyn Executor<'a>>,
        on: Expr,
        join_type: JoinType,
    ) -> Self {
        Self {
            left,
            right,
            on,
            join_type,
            right_buf: Vec::new(),
            left_row: None,
            right_pos: 0,
        }
    }
}

impl<'a> Executor<'a> for JoinExecutor<'a> {
    fn open(&mut self, ctx: &ExecutionContext) -> ExecResult<()> {
        self.right_buf.clear();
        self.left_row = None;
        self.right_pos = 0;

        self.left.open(ctx)?;
        self.right.open(ctx)?;

        while let Some(row) = self.right.next(ctx)? {
            self.right_buf.push(row);
        }

        Ok(())
    }

    fn next(&mut self, ctx: &ExecutionContext) -> ExecResult<Option<Row>> {
        loop {
            if self.left_row.is_none() {
                self.left_row = self.left.next(ctx)?;
                self.right_pos = 0;

                if self.left_row.is_none() {
                    return Ok(None);
                }
            }

            let left = self.left_row.as_ref().unwrap();

            while self.right_pos < self.right_buf.len() {
                let right = &self.right_buf[self.right_pos];
                self.right_pos += 1;

                let mut joined = Vec::with_capacity(left.len() + right.len());
                joined.extend_from_slice(left);
                joined.extend_from_slice(right);

                match eval_expr(&self.on, &joined)? {
                    Value::Boolean(true) => return Ok(Some(joined)),
                    Value::Boolean(false) | Value::Null => continue,
                    _ => {
                        return Err(ExecutionError::InvalidExpression {
                            reason: "join predicate must be boolean".into(),
                        });
                    }
                }
            }

            self.left_row = None;
        }
    }

    fn close(&mut self, ctx: &ExecutionContext) -> ExecResult<Vec<TableMutationStats>> {
        self.right_buf.clear();
        self.left.close(ctx)?;
        self.right.close(ctx)?;
        Ok(vec![])
    }
}
