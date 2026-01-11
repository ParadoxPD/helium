use std::cmp::Ordering;

use crate::execution::context::ExecutionContext;
use crate::execution::errors::TableMutationStats;
use crate::execution::eval_expr::eval_expr;
use crate::execution::executor::{ExecResult, Executor, Row};
use crate::ir::plan::SortKey;
use crate::types::value::Value;

pub struct SortExecutor {
    input: Box<dyn Executor>,
    keys: Vec<SortKey>,
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
    fn open(&mut self, ctx: &mut ExecutionContext) -> ExecResult<()> {
        self.buffer.clear();
        self.pos = 0;
        self.input.open(ctx)?;

        while let Some(row) = self.input.next(ctx)? {
            self.buffer.push(row);
        }

        let keys = self.keys.clone();
        self.buffer.sort_by(|a, b| compare_rows(a, b, &keys));

        Ok(())
    }

    fn next(&mut self, _ctx: &mut ExecutionContext) -> ExecResult<Option<Row>> {
        if self.pos >= self.buffer.len() {
            return Ok(None);
        }

        let row = self.buffer[self.pos].clone();
        self.pos += 1;
        Ok(Some(row))
    }

    fn close(&mut self, ctx: &mut ExecutionContext) -> ExecResult<Vec<TableMutationStats>> {
        self.buffer.clear();
        self.pos = 0;
        self.input.close(ctx)
    }
}

fn compare_rows(a: &Row, b: &Row, keys: &[SortKey]) -> Ordering {
    for key in keys {
        let va = eval_expr(&key.expr, a).unwrap_or(Value::Null);
        let vb = eval_expr(&key.expr, b).unwrap_or(Value::Null);

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
        (Value::Null, Value::Null) => Equal,
        (Value::Null, _) => Greater,
        (_, Value::Null) => Less,
        _ => a.partial_cmp(b).unwrap_or(Equal),
    }
}
