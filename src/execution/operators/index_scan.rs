use std::sync::{Arc, Mutex};

use crate::execution::context::ExecutionContext;
use crate::execution::errors::{ExecutionError, TableMutationStats};
use crate::execution::executor::{ExecResult, Executor, Row};
use crate::ir::index_predicate::IndexPredicate;
use crate::storage::heap::heap_table::HeapTable;
use crate::storage::index::btree::key::IndexKey;
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
    fn open(&mut self, ctx: &mut ExecutionContext) -> ExecResult<()> {
        self.pos = 0;
        ctx.stats.index_lookups += 1;

        let idx = self.index.lock().unwrap();
        self.rids = match &self.predicate {
            IndexPredicate::Eq(v) => {
                let k = IndexKey::try_from(v)
                    .map_err(|e| ExecutionError::InvalidExpression { reason: e.into() })?;
                idx.get(&k)?
            }
            IndexPredicate::Range { low, high } => {
                let l = IndexKey::try_from(low)
                    .map_err(|e| ExecutionError::InvalidExpression { reason: e.into() })?;
                let h = IndexKey::try_from(high)
                    .map_err(|e| ExecutionError::InvalidExpression { reason: e.into() })?;
                idx.range(&l, &h)?
            }
        };

        Ok(())
    }

    fn next(&mut self, _ctx: &mut ExecutionContext) -> ExecResult<Option<Row>> {
        if self.pos >= self.rids.len() {
            return Ok(None);
        }

        let rid = self.rids[self.pos];
        self.pos += 1;

        let row = self.heap.fetch(rid)?;
        Ok(Some(row.values.clone()))
    }

    fn close(&mut self, _ctx: &mut ExecutionContext) -> ExecResult<Vec<TableMutationStats>> {
        self.rids.clear();
        Ok(vec![])
    }
}

