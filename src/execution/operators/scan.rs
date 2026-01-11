use crate::{
    catalog::ids::TableId,
    db_trace,
    diagnostics::debugger::Component,
    execution::{
        context::ExecutionContext,
        errors::{ExecutionError, TableMutationStats},
        executor::{ExecResult, Executor, Row},
    },
    storage::heap::{heap_cursor::HeapCursor, heap_table::HeapTable},
};

pub struct ScanExecutor<'a> {
    table_id: TableId,
    cursor: Option<HeapCursor<'a>>,
}

impl<'a> ScanExecutor<'a> {
    pub fn new(table_id: TableId) -> Self {
        Self {
            table_id,
            cursor: None,
        }
    }
}

impl<'a> Executor<'a> for ScanExecutor<'a> {
    fn open(&mut self, ctx: &ExecutionContext) -> ExecResult<()> {
        let table_meta =
            ctx.catalog
                .get_table_by_id(self.table_id)
                .ok_or(ExecutionError::TableNotFound {
                    table_id: self.table_id,
                })?;

        let heap = HeapTable::open(table_meta.id, ctx.buffer_pool.clone());
        self.cursor = Some(heap.scan()?);
        Ok(())
    }

    fn next(&mut self, ctx: &'a ExecutionContext) -> ExecResult<Option<Row>> {
        let cursor = self.cursor.as_mut().ok_or(ExecutionError::Internal(
            "scan cursor not initialized".into(),
        ))?;

        match cursor.next() {
            Some((_rid, row)) => {
                ctx.stats.rows_scanned += 1;
                Ok(Some(row.values.clone()))
            }
            None => Ok(None),
        }
    }

    fn close(&mut self, _ctx: &ExecutionContext) -> ExecResult<Vec<TableMutationStats>> {
        self.cursor = None;
        Ok(vec![])
    }
}
