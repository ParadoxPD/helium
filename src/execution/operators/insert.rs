use crate::catalog::ids::TableId;
use crate::execution::context::ExecutionContext;
use crate::execution::errors::{ExecutionError, TableMutationStats};
use crate::execution::eval_expr::eval_expr;
use crate::execution::executor::{ExecResult, Executor, Row};
use crate::ir::expr::Expr;
use crate::storage::heap::heap_table::HeapTable;
use crate::storage::index::btree::key::IndexKey;

pub struct InsertExecutor {
    table_id: TableId,
    rows: Vec<Vec<Expr>>,
    pos: usize,
    stats: TableMutationStats,
}

impl InsertExecutor {
    pub fn new(table_id: TableId, rows: Vec<Vec<Expr>>) -> Self {
        Self {
            table_id,
            rows,
            pos: 0,
            stats: TableMutationStats::new(table_id),
        }
    }
}

impl<'a> Executor<'a> for InsertExecutor {
    fn open(&mut self, _ctx: &ExecutionContext) -> ExecResult<()> {
        self.pos = 0;
        Ok(())
    }

    fn next(&mut self, ctx: &ExecutionContext) -> ExecResult<Option<Row>> {
        if self.pos >= self.rows.len() {
            return Ok(None);
        }

        let table_meta =
            ctx.catalog
                .get_table_by_id(self.table_id)
                .ok_or(ExecutionError::TableNotFound {
                    table_id: self.table_id,
                })?;

        let heap = HeapTable::open(table_meta.id, ctx.buffer_pool.clone());
        let exprs = &self.rows[self.pos];
        self.pos += 1;

        let mut values = Vec::with_capacity(exprs.len());
        for e in exprs {
            values.push(eval_expr(e, &[])?);
        }

        let rid = heap.insert(values.clone())?;
        self.stats.rows_inserted += 1;
        self.stats.rows_affected += 1;

        for idx in ctx.catalog.indexes_for_table(self.table_id) {
            let col = idx.meta.column_ids[0];
            let key = IndexKey::try_from(&values[col.0 as usize]).map_err(|e| {
                ExecutionError::IndexViolation {
                    index_id: idx.meta.id,
                    reason: e.into(),
                }
            })?;

            idx.index.lock().unwrap().insert(key, rid)?;
            self.stats.record_index_insert(idx.meta.id);
        }

        Ok(None)
    }

    fn close(&mut self, _ctx: &ExecutionContext) -> ExecResult<Vec<TableMutationStats>> {
        Ok(vec![self.stats.clone()])
    }
}
