use crate::{
    catalog::ids::TableId,
    execution::{
        context::ExecutionContext,
        errors::{ExecutionError, TableMutationStats},
        eval_expr::eval_expr,
        executor::{ExecResult, Executor, Row},
    },
    ir::expr::Expr,
    storage::index::btree::key::IndexKey,
    types::value::Value,
};

pub struct DeleteExecutor {
    table_id: TableId,
    predicate: Option<Expr>,
    done: bool,
    stats: TableMutationStats,
}

impl DeleteExecutor {
    pub fn new(table_id: TableId, predicate: Option<Expr>) -> Self {
        Self {
            table_id,
            predicate,
            done: false,
            stats: TableMutationStats::new(table_id),
        }
    }
}

impl Executor for DeleteExecutor {
    fn open(&mut self, _ctx: &mut ExecutionContext) -> ExecResult<()> {
        self.done = false;
        Ok(())
    }

    fn next(&mut self, ctx: &mut ExecutionContext) -> ExecResult<Option<Row>> {
        if self.done {
            return Ok(None);
        }

        self.done = true;

        let _table_meta =
            ctx.catalog
                .get_table_by_id(self.table_id)
                .ok_or(ExecutionError::TableNotFound {
                    table_id: self.table_id,
                })?;

        let heap = ctx.get_heap(self.table_id)?;
        let cursor = heap.scan();

        let mut to_delete = Vec::new();

        for (rid, row) in cursor {
            if let Some(pred) = &self.predicate {
                match eval_expr(pred, &row.values)? {
                    Value::Boolean(true) => {}
                    _ => continue,
                }
            }
            to_delete.push((rid, row.values.clone()));
        }

        for (rid, old_row) in to_delete {
            for idx in ctx.catalog.indexes_for_table(self.table_id) {
                let col = idx.meta.column_ids[0];
                let key = IndexKey::try_from(&old_row[col.0 as usize]).map_err(|e| {
                    ExecutionError::IndexViolation {
                        index_id: idx.meta.id,
                        reason: e.into(),
                    }
                })?;

                idx.index.lock().unwrap().delete(&key, rid)?;
                self.stats.record_index_delete(idx.meta.id);
            }

            heap.delete(rid)?;
            self.stats.rows_deleted += 1;
            self.stats.rows_affected += 1;
        }

        Ok(None)
    }

    fn close(&mut self, _ctx: &mut ExecutionContext) -> ExecResult<Vec<TableMutationStats>> {
        Ok(vec![self.stats.clone()])
    }
}

