use crate::catalog::ids::{IndexId, TableId};
use crate::execution::context::ExecutionContext;
use crate::execution::executor::{Executor, Row};
use crate::ir::index_predicate::IndexPredicate;
use crate::storage::page::RowId;

pub struct IndexScanExecutor {
    table_id: TableId,
    index_id: IndexId,
    predicate: IndexPredicate,

    // runtime state
    rids: Vec<RowId>,
    pos: usize,
}

impl IndexScanExecutor {
    pub fn new(table_id: TableId, index_id: IndexId, predicate: IndexPredicate) -> Self {
        Self {
            table_id,
            index_id,
            predicate,
            rids: Vec::new(),
            pos: 0,
        }
    }
}

impl Executor for IndexScanExecutor {
    fn open(&mut self, ctx: &ExecutionContext) {
        self.rids.clear();
        self.pos = 0;

        let index = ctx
            .catalog
            .get_index_by_id(self.index_id)
            .expect("index must exist");

        let index_handle = index.handle(); // adapt to your API

        match &self.predicate {
            IndexPredicate::Eq(v) => {
                let key = index_handle.make_key(v);
                self.rids = index_handle.get(&key);
            }

            IndexPredicate::Range { low, high } => {
                let low_k = index_handle.make_key(low);
                let high_k = index_handle.make_key(high);
                self.rids = index_handle.range(&low_k, &high_k);
            }
        }
    }

    fn next(&mut self) -> Option<Row> {
        if self.pos >= self.rids.len() {
            return None;
        }

        let rid = self.rids[self.pos];
        self.pos += 1;

        let table = ctx
            .catalog
            .get_table_by_id(self.table_id)
            .expect("table must exist");

        let heap = table.heap();
        let storage_row = heap.fetch(rid);

        // IMPORTANT:
        // storage_row.values is Vec<Value> in schema order
        Some(storage_row.values.clone())
    }

    fn close(&mut self) {
        self.rids.clear();
        self.pos = 0;
    }
}

