use crate::{common::value::Value, exec::evaluator::ExecError, storage::page::RowId};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub row_id: RowId,
    pub values: HashMap<String, Value>,
}
impl Default for Row {
    fn default() -> Self {
        Self {
            row_id: RowId::default(),
            values: HashMap::new(),
        }
    }
}

pub trait Operator {
    fn open(&mut self) -> Result<(), ExecError>;
    fn next(&mut self) -> Result<Option<Row>, ExecError>;
    fn close(&mut self) -> Result<(), ExecError>;
}

#[derive(Default, Debug)]
pub struct ExecStats {
    pub rows: usize,
    pub elapsed_ns: u128,
}

pub fn lower(plan: &LogicalPlan, catalog: &Catalog) -> Box<dyn Operator> {
    match plan {
        LogicalPlan::Scan(scan) => {
            let table = catalog.get_table(&scan.table).expect("table not found");
            let alias = scan.alias.clone();

            let scan_exec = ScanExec::new(table.heap.clone(), alias, scan.columns.clone());
            Box::new(scan_exec)
        }

        LogicalPlan::Filter(filter) => {
            let input = lower(&filter.input, catalog);
            Box::new(FilterExec::new(input, filter.predicate.clone()))
        }

        LogicalPlan::Project(project) => {
            let input = lower(&project.input, catalog);
            Box::new(ProjectExec::new(input, project.exprs.clone()))
        }

        LogicalPlan::Sort(sort) => {
            let input = lower(&sort.input, catalog);
            Box::new(SortExec::new(input, sort.keys.clone()))
        }

        LogicalPlan::Limit(limit) => {
            let input = lower(&limit.input, catalog);
            Box::new(LimitExec::new(input, limit.count))
        }
        LogicalPlan::Join(join) => {
            let left = lower(&join.left, catalog);
            let right = lower(&join.right, catalog);

            Box::new(JoinExec::new(left, right, join.on.clone()))
        }
        LogicalPlan::IndexScan(scan) => {
            let table = catalog.get_table(&scan.table).unwrap().clone();

            let index = catalog
                .get_index(&scan.table, &scan.column)
                .expect("optimizer promised index")
                .clone();

            Box::new(IndexScanExec::new(
                table.heap.clone(),
                table.name.clone(),
                index,
                scan.predicate.clone(),
                scan.column.clone(),
                table.heap.schema().clone(),
            ))
        }
    }
}

pub fn execute_plan(plan: LogicalPlan, catalog: &Catalog) -> Result<QueryResult, anyhow::Error> {
    let mut op = lower(&plan, catalog);

    op.open()?;

    let mut rows = Vec::new();
    while let Some(row) = op.next()? {
        rows.push(row);
    }

    op.close()?;

    Ok(QueryResult::Rows(rows))
}

pub fn execute_delete(del: BoundDelete, catalog: &Catalog) -> Result<usize, anyhow::Error> {
    let table = catalog
        .get_table(&del.table)
        .ok_or_else(|| anyhow::anyhow!("table '{}' not found", del.table))?;

    let mut cursor = table.heap.scan();

    let mut to_delete = Vec::new();

    while let Some((rid, storage_row)) = cursor.next() {
        let row = Row {
            row_id: rid,
            values: materialize_row(&del.table, &storage_row.values, &table.schema)?,
        };

        let ev = Evaluator::new(&row);

        if let Some(pred) = &del.predicate {
            if !ev.eval_predicate(pred)? {
                continue;
            }
        }

        to_delete.push(rid);
    }
    let rows = to_delete.len();

    for rid in to_delete {
        table.heap.delete(rid);
    }

    Ok(rows)
}

pub fn execute_update(upd: BoundUpdate, catalog: &Catalog) -> Result<usize, anyhow::Error> {
    let table = catalog
        .get_table(&upd.table)
        .ok_or_else(|| anyhow::anyhow!("table '{}' not found", upd.table))?;

    let mut cursor = table.heap.scan();

    let mut to_update = Vec::new();

    while let Some((rid, storage_row)) = cursor.next() {
        let row = Row {
            row_id: rid,
            values: materialize_row(&upd.table, &storage_row.values, &table.schema)?,
        };

        let ev = Evaluator::new(&row);

        if let Some(pred) = &upd.predicate {
            if !ev.eval_predicate(pred)? {
                continue;
            }
        }

        let mut updated = row.values.clone();
        for (col, expr) in &upd.assignments {
            let v = ev.eval_expr(expr)?.unwrap_or(Value::Null);
            let key = format!("{}.{}", upd.table, col.name);
            updated.insert(key, v);
        }

        to_update.push((rid, updated));
    }

    let rows = to_update.len();

    for (rid, updated) in to_update {
        let physical = table
            .schema
            .columns
            .iter()
            .map(|c| {
                let key = format!("{}.{}", upd.table, c.name);
                updated.get(&key).cloned().unwrap_or(Value::Null)
            })
            .collect::<Vec<_>>();

        table.heap.delete(rid);
        table.heap.insert(physical);
    }

    Ok(rows)
}

pub fn materialize_row(
    table: &str,
    values: &[Value],
    schema: &Schema,
) -> Result<HashMap<String, Value>> {
    if values.len() != schema.columns.len() {
        bail!(
            "row/schema length mismatch: {} values, {} columns",
            values.len(),
            schema.columns.len()
        );
    }

    let mut map = HashMap::with_capacity(values.len());

    for (col, val) in schema.columns.iter().zip(values.iter()) {
        map.insert(format!("{}.{}", table, col.name), val.clone());
    }

    Ok(map)
}
