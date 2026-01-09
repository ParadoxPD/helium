use crate::catalog::ids::TableId;
use crate::execution::context::ExecutionContext;
use crate::execution::executor::{Executor, Row};
use crate::ir::expr::Expr;

pub struct InsertExecutor {
    pub(crate) table_id: TableId,
    pub(crate) rows: Vec<Vec<Expr>>,

    // runtime
    done: bool,
    pub(crate) inserted: usize,
}

impl InsertExecutor {
    pub fn new(table_id: TableId, rows: Vec<Vec<Expr>>) -> Self {
        Self {
            table_id,
            rows,
            done: false,
            inserted: 0,
        }
    }
}

impl Executor for InsertExecutor {
    fn open(&mut self, _ctx: &ExecutionContext) {
        self.done = false;
        self.inserted = 0;
    }

    fn next(&mut self) -> Option<Row> {
        if self.done {
            return None;
        }

        self.done = true;

        // NOTE:
        // actual insertion happens in close()
        None
    }

    fn close(&mut self) {
        // No-op here; actual work is done by engine
    }
}
