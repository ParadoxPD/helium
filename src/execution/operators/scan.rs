use crate::{
    catalog::ids::TableId,
    db_trace,
    diagnostics::debugger::Component,
    execution::{
        context::ExecutionContext,
        executor::{Executor, Row},
    },
    storage::heap::table::{HeapCursor, HeapTable},
};

pub struct ScanExecutor {
    table_id: TableId,
    cursor: Option<HeapCursor>,
}

impl ScanExecutor {
    pub fn new(table_id: TableId) -> Self {
        Self {
            table_id,
            cursor: None,
        }
    }
}

impl Executor for ScanExecutor {
    fn open(&mut self, ctx: &ExecutionContext) {
        db_trace!(
            Component::Executor,
            "Opening scan on table '{}'",
            self.alias
        );
        self.cursor = Some(self.table.clone().scan());
        let table = ctx
            .catalog
            .get_table_by_id(self.table_id)
            .expect("table must exist at execution time");

        // IMPORTANT: execution only grabs the heap handle
        let heap: &HeapTable = table.heap(); // adapt to your API
        self.cursor = Some(heap.scan());
    }

    fn next(&mut self) -> Option<Row> {
        db_trace!(Component::Executor, "ScanExec::next() on '{}'", self.alias);
        let cursor = self.cursor.as_mut()?;

        let (row_id, storage_row) = cursor.next()?;
        db_trace!(
            Component::Executor,
            "Found row: row id = {:?}, values = {:?}",
            row_id,
            storage_row.values
        );

        // IMPORTANT INVARIANT:
        // storage_row.values is already Vec<Value> in schema order
        Some(storage_row.values.clone())
    }

    fn close(&mut self) {
        self.cursor = None;
    }
}
