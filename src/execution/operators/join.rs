use crate::execution::context::ExecutionContext;
use crate::execution::eval_expr::eval_expr;
use crate::execution::executor::{Executor, Row};
use crate::ir::expr::Expr;
use crate::ir::plan::JoinType;
use crate::types::value::Value;

pub struct JoinExecutor {
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    on: Expr,
    join_type: JoinType,

    // runtime state
    right_buffer: Vec<Row>,
    left_row: Option<Row>,
    right_pos: usize,
}

impl JoinExecutor {
    pub fn new(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        on: Expr,
        join_type: JoinType,
    ) -> Self {
        Self {
            left,
            right,
            on,
            join_type,
            right_buffer: Vec::new(),
            left_row: None,
            right_pos: 0,
        }
    }
}

impl Executor for JoinExecutor {
    fn open(&mut self, ctx: &ExecutionContext) {
        self.right_buffer.clear();
        self.left_row = None;
        self.right_pos = 0;

        self.left.open(ctx);
        self.right.open(ctx);

        // Materialize RIGHT side
        while let Some(row) = self.right.next() {
            self.right_buffer.push(row);
        }
    }

    fn next(&mut self) -> Option<Row> {
        loop {
            // Fetch next left row if needed
            if self.left_row.is_none() {
                self.left_row = self.left.next();
                self.right_pos = 0;

                if self.left_row.is_none() {
                    return None; // no more rows
                }
            }

            let left = self.left_row.as_ref().unwrap();

            while self.right_pos < self.right_buffer.len() {
                let right = &self.right_buffer[self.right_pos];
                self.right_pos += 1;

                // Combine rows
                let mut joined = Vec::with_capacity(left.len() + right.len());
                joined.extend_from_slice(left);
                joined.extend_from_slice(right);

                let pred = eval_expr(&self.on, &joined);

                match pred {
                    Value::Boolean(true) => return Some(joined),
                    Value::Boolean(false) | Value::Null => continue,
                    other => panic!("Join predicate did not evaluate to boolean: {:?}", other),
                }
            }

            // Exhausted right side for this left row
            self.left_row = None;
        }
    }

    fn close(&mut self) {
        self.right_buffer.clear();
        self.left_row = None;
        self.right_pos = 0;

        self.left.close();
        self.right.close();
    }
}
