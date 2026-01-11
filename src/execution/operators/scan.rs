use crate::{
    catalog::ids::TableId,
    execution::{
        context::ExecutionContext,
        errors::{ExecutionError, TableMutationStats},
        executor::{ExecResult, Executor, Row},
    },
    storage::heap::heap_cursor::HeapCursor,
};
use std::sync::Arc;

pub struct ScanExecutor {
    table_id: TableId,
    heap: Option<Arc<crate::storage::heap::heap_table::HeapTable>>,
    position: usize,
    rows: Vec<Row>,
}

impl ScanExecutor {
    pub fn new(table_id: TableId) -> Self {
        Self {
            table_id,
            heap: None,
            position: 0,
            rows: Vec::new(),
        }
    }
}

impl Executor for ScanExecutor {
    fn open(&mut self, ctx: &mut ExecutionContext) -> ExecResult<()> {
        let _table_meta =
            ctx.catalog
                .get_table_by_id(self.table_id)
                .ok_or(ExecutionError::TableNotFound {
                    table_id: self.table_id,
                })?;

        let heap = ctx.get_heap(self.table_id)?;

        // Materialize all rows during open
        self.rows.clear();
        for (_rid, row) in heap.scan() {
            self.rows.push(row.values.clone());
        }

        self.heap = Some(heap);
        self.position = 0;
        Ok(())
    }

    fn next(&mut self, ctx: &mut ExecutionContext) -> ExecResult<Option<Row>> {
        if self.position >= self.rows.len() {
            return Ok(None);
        }

        let row = self.rows[self.position].clone();
        self.position += 1;
        ctx.stats.rows_scanned += 1;
        Ok(Some(row))
    }

    fn close(&mut self, _ctx: &mut ExecutionContext) -> ExecResult<Vec<TableMutationStats>> {
        self.heap = None;
        self.rows.clear();
        Ok(vec![])
    }
}

