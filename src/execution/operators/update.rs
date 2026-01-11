use crate::catalog::ids::{ColumnId, TableId};
use crate::execution::context::ExecutionContext;
use crate::execution::errors::TableMutationStats;
use crate::execution::executor::{ExecResult, Executor, Row};
use crate::ir::expr::Expr;

pub struct UpdateExecutor {
    pub(crate) table_id: TableId,
    pub(crate) assignments: Vec<(ColumnId, Expr)>,
    pub(crate) predicate: Option<Expr>,

    // runtime
    done: bool,
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
        }
    }
}

impl Executor for UpdateExecutor {
    fn open(&mut self, _ctx: &mut ExecutionContext) -> ExecResult<()> {
        self.done = false;
        Ok(())
    }

    fn next(&mut self, _ctx: &mut ExecutionContext) -> ExecResult<Option<Row>> {
        // UPDATE produces no rows
        if self.done {
            Ok(None)
        } else {
            self.done = true;
            Ok(None)
        }
    }

    fn close(&mut self, _ctx: &mut ExecutionContext) -> ExecResult<Vec<TableMutationStats>> {
        // Actual UPDATE is performed by engine
        Ok(vec![])
    }
}
