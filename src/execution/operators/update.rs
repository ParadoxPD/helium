use crate::catalog::ids::{ColumnId, TableId};
use crate::execution::context::ExecutionContext;
use crate::execution::executor::{Executor, Row};
use crate::ir::expr::Expr;

pub struct UpdateExecutor {
    pub(crate) table_id: TableId,
    pub(crate) assignments: Vec<(ColumnId, Expr)>,
    pub(crate) predicate: Option<Expr>,

    // runtime
    done: bool,
    pub(crate) updated: usize,
}

impl UpdateExecutor {
    pub fn new(
        table_id: TableId,
        assignments: Vec<(ColumnId, Expr)>,
        predicate: Option<Expr>,
    ) -> Self {
        Self {
            table_id,
            assignments,
            predicate,
            done: false,
            updated: 0,
        }
    }
}

impl Executor for UpdateExecutor {
    fn open(&mut self, _ctx: &ExecutionContext) {
        self.done = false;
        self.updated = 0;
    }

    fn next(&mut self) -> Option<Row> {
        // UPDATE produces no rows
        if self.done {
            None
        } else {
            self.done = true;
            None
        }
    }

    fn close(&mut self) {
        // actual update happens in engine
    }
}

