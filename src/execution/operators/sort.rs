use crate::execution::context::ExecutionContext;
use crate::execution::eval_expr::eval_expr;
use crate::execution::executor::{Executor, Row};
use crate::ir::plan::SortKey;
use crate::types::value::Value;

pub struct SortExecutor {
    input: Box<dyn Executor>,
    keys: Vec<SortKey>,

    // runtime state
    buffer: Vec<Row>,
    pos: usize,
}

impl SortExecutor {
    pub fn new(input: Box<dyn Executor>, keys: Vec<SortKey>) -> Self {
        Self {
            input,
            keys,
            buffer: Vec::new(),
            pos: 0,
        }
    }
}

impl Executor for SortExecutor {
    fn open(&mut self, ctx: &ExecutionContext) {
        self.buffer.clear();
        self.pos = 0;

        self.input.open(ctx);

        // 1. Drain child
        while let Some(row) = self.input.next() {
            self.buffer.push(row);
        }

        // 2. Sort buffer
        let keys = self.keys.clone();
        self.buffer.sort_by(|a, b| compare_rows(a, b, &keys));
    }

    fn next(&mut self) -> Option<Row> {
        if self.pos >= self.buffer.len() {
            return None;
        }

        let row = self.buffer[self.pos].clone();
        self.pos += 1;
        Some(row)
    }

    fn close(&mut self) {
        self.buffer.clear();
        self.pos = 0;
        self.input.close();
    }
}

use std::cmp::Ordering;

fn compare_rows(a: &Row, b: &Row, keys: &[SortKey]) -> Ordering {
    for key in keys {
        let va = eval_expr(&key.expr, a);
        let vb = eval_expr(&key.expr, b);

        let ord = compare_values(&va, &vb);

        if ord != Ordering::Equal {
            return if key.asc { ord } else { ord.reverse() };
        }
    }

    Ordering::Equal
}

fn compare_values(a: &Value, b: &Value) -> Ordering {
    use Ordering::*;

    match (a, b) {
        // NULLs sort last (Postgres / SQLite default)
        (Value::Null, Value::Null) => Equal,
        (Value::Null, _) => Greater,
        (_, Value::Null) => Less,

        _ => a.cmp(b).unwrap_or(Equal), // type mismatch should not happen
    }
}
