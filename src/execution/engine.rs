use crate::execution::context::ExecutionContext;
use crate::execution::eval_expr::eval_expr;
use crate::execution::executor::{Executor, Row};
use crate::execution::operators::delete::DeleteExecutor;
use crate::execution::operators::filter::FilterExecutor;
use crate::execution::operators::index_scan::IndexScanExecutor;
use crate::execution::operators::insert::InsertExecutor;
use crate::execution::operators::join::JoinExecutor;
use crate::execution::operators::limit::LimitExecutor;
use crate::execution::operators::project::ProjectExecutor;
use crate::execution::operators::scan::ScanExecutor;
use crate::execution::operators::sort::SortExecutor;
use crate::execution::operators::update::UpdateExecutor;
use crate::ir::plan::LogicalPlan;
use crate::storage::errors::StorageError;
use crate::storage::index::btree::key::IndexKey;
use crate::storage::page::row_id::RowId;
use crate::types::value::Value;

pub enum ExecutionResult {
    Rows(Vec<Row>),
    Affected(usize),
}

pub fn execute_plan(plan: LogicalPlan, ctx: &ExecutionContext) -> ExecutionResult {
    match plan {
        LogicalPlan::Insert { table_id, rows } => {
            let mut exec = InsertExecutor::new(table_id, rows);
            exec.open(ctx);
            let count = execute_insert(&mut exec, ctx);
            exec.close();
            ExecutionResult::Affected(count)
        }

        LogicalPlan::Delete {
            table_id,
            predicate,
        } => {
            let mut exec = DeleteExecutor::new(table_id, predicate);
            exec.open(ctx);
            let count = execute_delete(&mut exec, ctx);
            exec.close();
            ExecutionResult::Affected(count)
        }

        LogicalPlan::Update {
            table_id,
            assignments,
            predicate,
        } => {
            let mut exec = UpdateExecutor::new(table_id, assignments, predicate);
            exec.open(ctx);
            let count = execute_update(&mut exec, ctx);
            exec.close();
            ExecutionResult::Affected(count)
        }
        LogicalPlan::IndexScan {
            table_id,
            index_id,
            predicate,
        } => {
            let table = ctx
                .catalog
                .get_table_by_id(*table_id)
                .expect("table must exist");

            let index_entry = ctx
                .catalog
                .get_index_by_id(*index_id)
                .expect("index must exist");

            Box::new(IndexScanExecutor::new(
                table.heap(),
                index_entry.index.clone(),
                predicate.clone(),
            ))
        }

        _ => {
            let mut root = build_executor(plan, ctx);
            root.open(ctx);

            let mut rows = Vec::new();
            while let Some(row) = root.next() {
                rows.push(row);
            }

            root.close();
            ExecutionResult::Rows(rows)
        }
    }
}

pub fn build_executor(plan: LogicalPlan, ctx: &ExecutionContext) -> Box<dyn Executor> {
    match plan {
        LogicalPlan::Scan { table_id } => Box::new(ScanExecutor::new(table_id)),

        LogicalPlan::Filter { input, predicate } => {
            let child = build_executor(*input, ctx);
            Box::new(FilterExecutor::new(child, predicate))
        }

        LogicalPlan::Project { input, exprs } => {
            let child = build_executor(*input, ctx);
            Box::new(ProjectExecutor::new(child, exprs))
        }

        LogicalPlan::Sort { input, keys } => {
            let child = build_executor(*input, ctx);
            Box::new(SortExecutor::new(child, keys))
        }

        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let child = build_executor(*input, ctx);
            Box::new(LimitExecutor::new(child, limit, offset))
        }

        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
        } => {
            let l = build_executor(*left, ctx);
            let r = build_executor(*right, ctx);
            Box::new(JoinExecutor::new(l, r, on, join_type))
        }

        LogicalPlan::Insert { table_id, rows } => Box::new(InsertExecutor::new(table_id, rows)),

        LogicalPlan::Update {
            table_id,
            assignments,
            predicate,
        } => Box::new(UpdateExecutor::new(table_id, assignments, predicate)),

        LogicalPlan::Delete {
            table_id,
            predicate,
        } => Box::new(DeleteExecutor::new(table_id, predicate)),
    }
}

pub fn execute_insert(exec: &mut InsertExecutor, ctx: &ExecutionContext) -> StorageResult<usize> {
    let table = ctx.catalog.get_table_by_id(exec.table_id).unwrap();
    let heap = table.heap;

    for row_exprs in &exec.rows {
        let mut values = Vec::with_capacity(row_exprs.len());

        for expr in row_exprs {
            values.push(eval_expr(expr, &[]));
        }

        let rid = heap.insert(values.clone());

        for index_entry in ctx.catalog.indexes_for_table(exec.table_id) {
            let col_id = index_entry.meta.column_ids[0];
            let key = IndexKey::try_from(&values[col_id.0 as usize]).map_err(|e| {
                StorageError::IndexViolation {
                    index_name: self.name.clone(),
                    reason: e.to_string(),
                }
            })?;

            index_entry.index.lock().unwrap().insert(key, rid);
        }

        exec.inserted += 1;
    }

    exec.inserted
}

pub fn execute_delete(exec: &mut DeleteExecutor, ctx: &ExecutionContext) -> usize {
    let table = ctx.catalog.get_table_by_id(exec.table_id).unwrap();
    let heap = table.heap();
    let mut cursor = heap.scan();

    let mut to_delete: Vec<(RowId, Vec<Value>)> = Vec::new();

    while let Some((rid, storage_row)) = cursor.next() {
        let row = storage_row.values.clone();

        if let Some(pred) = &exec.predicate {
            match eval_expr(pred, &row) {
                Value::Boolean(true) => {}
                Value::Boolean(false) | Value::Null => continue,
                _ => panic!("DELETE predicate not boolean"),
            }
        }

        to_delete.push((rid, row));
    }

    for (rid, old_row) in to_delete {
        for index_entry in ctx.catalog.indexes_for_table(exec.table_id) {
            let col_id = index_entry.meta.column_ids[0];
            let key = IndexKey::try_from(&old_row[col_id.0 as usize]).map_err(|e| {
                StorageError::IndexViolation {
                    index_name: self.name.clone(),
                    reason: e.to_string(),
                }
            })?;

            index_entry.index.lock().unwrap().delete(&key, rid);
        }

        heap.delete(rid);
        exec.deleted += 1;
    }

    exec.deleted
}

pub fn execute_update(exec: &mut UpdateExecutor, ctx: &ExecutionContext) -> usize {
    let table = ctx.catalog.get_table_by_id(exec.table_id).unwrap();
    let heap = table.heap();
    let mut cursor = heap.scan();

    let mut updates = Vec::new();

    while let Some((rid, storage_row)) = cursor.next() {
        let old_row = storage_row.values.clone();

        if let Some(pred) = &exec.predicate {
            match eval_expr(pred, &old_row) {
                Value::Boolean(true) => {}
                Value::Boolean(false) | Value::Null => continue,
                _ => panic!("UPDATE predicate not boolean"),
            }
        }

        let mut new_row = old_row.clone();
        for (col_id, expr) in &exec.assignments {
            new_row[col_id.0 as usize] = eval_expr(expr, &old_row);
        }

        updates.push((rid, old_row, new_row));
    }

    for (rid, old_row, new_row) in updates {
        for index_entry in ctx.catalog.indexes_for_table(exec.table_id) {
            let col_id = index_entry.meta.column_ids[0];

            let old_key = &old_row[col_id.0 as usize];
            let new_key = &new_row[col_id.0 as usize];

            let mut idx = index_entry.index.lock().unwrap();
            idx.delete(old_key, rid);
            idx.insert(new_key, rid);
        }

        heap.delete(rid);
        heap.insert(new_row);
        exec.updated += 1;
    }

    exec.updated
}
