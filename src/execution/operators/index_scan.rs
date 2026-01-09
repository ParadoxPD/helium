use std::sync::{Arc, Mutex};

use crate::execution::executor::{Executor, Row};
use crate::ir::index_predicate::IndexPredicate;
use crate::storage::heap::heap_table::HeapTable;
use crate::storage::index::index::Index;
use crate::storage::page::row_id::RowId;

pub struct IndexScanExecutor {
    index: Arc<Mutex<dyn Index>>,
    heap: Arc<HeapTable>,
    predicate: IndexPredicate,

    rids: Vec<RowId>,
    pos: usize,
}

impl IndexScanExecutor {
    pub fn new(
        index: Arc<Mutex<dyn Index>>,
        heap: Arc<HeapTable>,
        predicate: IndexPredicate,
    ) -> Self {
        Self {
            index,
            heap,
            predicate,
            rids: Vec::new(),
            pos: 0,
        }
    }
}

impl Executor for IndexScanExecutor {
    fn open(&mut self) {
        self.pos = 0;
        let idx = self.index.lock().unwrap();

        self.rids = match &self.predicate {
            IndexPredicate::Eq(v) => idx.get(v),
            IndexPredicate::Range { low, high } => idx.range(low, high),
        };
    }

    fn next(&mut self) -> Option<Row> {
        if self.pos >= self.rids.len() {
            return None;
        }

        let rid = self.rids[self.pos];
        self.pos += 1;

        let storage_row = self.heap.fetch(rid);

        Some(storage_row.values.clone())
    }

    fn close(&mut self) {
        self.rids.clear();
        self.pos = 0;
    }
}
