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
        } => Box::new(IndexScanExecutor::new(table_id, index_id, predicate)),

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

pub fn execute_insert(exec: &mut InsertExecutor, ctx: &ExecutionContext) -> usize {
    let table = ctx
        .catalog
        .get_table_by_id(exec.table_id)
        .expect("table must exist");

    let heap = table.heap(); // adapt to your API

    for row_exprs in &exec.rows {
        let mut values = Vec::with_capacity(row_exprs.len());

        for expr in row_exprs {
            let v = eval_expr(expr, &[]);
            values.push(v);
        }

        heap.insert(values);
        exec.inserted += 1;
    }

    exec.inserted
}

pub fn execute_delete(exec: &mut DeleteExecutor, ctx: &ExecutionContext) -> usize {
    let table = ctx
        .catalog
        .get_table_by_id(exec.table_id)
        .expect("table must exist");

    let heap = table.heap(); // adapt to your storage API
    let mut cursor = heap.scan();

    let mut to_delete = Vec::new();

    while let Some((row_id, storage_row)) = cursor.next() {
        let row = storage_row.values.clone(); // Vec<Value>

        if let Some(pred) = &exec.predicate {
            let v = eval_expr(pred, &row);

            match v {
                Value::Bool(true) => {}
                Value::Bool(false) | Value::Null => continue,
                other => panic!("DELETE predicate did not evaluate to boolean: {:?}", other),
            }
        }

        to_delete.push(row_id);
    }

    for rid in to_delete {
        heap.delete(rid);
        exec.deleted += 1;
    }

    exec.deleted
}

pub fn execute_update(exec: &mut UpdateExecutor, ctx: &ExecutionContext) -> usize {
    let table = ctx
        .catalog
        .get_table_by_id(exec.table_id)
        .expect("table must exist");

    let heap = table.heap(); // adapt to your storage API
    let mut cursor = heap.scan();

    // Collect updates first (avoid iterator invalidation)
    let mut updates: Vec<(/*row_id*/ _, /*new_values*/ Vec<Value>)> = Vec::new();

    while let Some((row_id, storage_row)) = cursor.next() {
        let old_row: Vec<Value> = storage_row.values.clone();

        // Predicate check (SQL semantics)
        if let Some(pred) = &exec.predicate {
            let v = eval_expr(pred, &old_row);
            match v {
                Value::Bool(true) => {}
                Value::Bool(false) | Value::Null => continue,
                other => panic!("UPDATE predicate did not evaluate to boolean: {:?}", other),
            }
        }

        // Start from old row and apply assignments
        let mut new_row = old_row.clone();
        for (col_id, expr) in &exec.assignments {
            let v = eval_expr(expr, &old_row);
            let idx = col_id.0 as usize;
            new_row[idx] = v;
        }

        updates.push((row_id, new_row));
    }

    // Apply updates
    for (row_id, new_values) in updates {
        // Phase 1 semantics: delete + insert
        // Phase 3 (MVCC): replace with versioned update
        heap.delete(row_id);
        heap.insert(new_values);
        exec.updated += 1;
    }

    exec.updated
}
