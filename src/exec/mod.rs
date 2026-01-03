pub mod catalog;
pub mod delete;
pub mod expr_eval;
pub mod filter;
pub mod index_scan;
pub mod join;
pub mod limit;
pub mod operator;
pub mod project;
pub mod scan;
pub mod sort;
pub mod unit_tests;
pub mod update;

use anyhow::{Result, bail};
use std::collections::HashMap;

use crate::api::db::QueryResult;
use crate::common::schema::Schema;
use crate::common::value::Value;
use crate::exec::catalog::Catalog;
use crate::exec::expr_eval::{eval_predicate, eval_value};
use crate::exec::filter::FilterExec;
use crate::exec::index_scan::IndexScanExec;
use crate::exec::join::JoinExec;
use crate::exec::limit::LimitExec;
use crate::exec::operator::{Operator, Row};
use crate::exec::project::ProjectExec;
use crate::exec::scan::ScanExec;
use crate::exec::sort::SortExec;
use crate::frontend::sql::binder::{BoundDelete, BoundInsert, BoundUpdate};
use crate::ir::plan::LogicalPlan;

pub fn lower(plan: &LogicalPlan, catalog: &Catalog) -> Box<dyn Operator> {
    match plan {
        LogicalPlan::Scan(scan) => {
            let table = catalog.get_table(&scan.table).expect("table not found");

            let scan_exec =
                ScanExec::new(table.heap.clone(), scan.alias.clone(), scan.columns.clone());
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

    op.open();

    let mut rows = Vec::new();
    while let Some(row) = op.next() {
        rows.push(row);
    }

    op.close();

    Ok(QueryResult::Rows(rows))
}

pub fn execute_delete(del: BoundDelete, catalog: &Catalog) -> Result<(), anyhow::Error> {
    let table = catalog
        .get_table(&del.table)
        .ok_or_else(|| anyhow::anyhow!("table '{}' not found", del.table))?;

    let mut cursor = table.heap.scan();

    while let Some((rid, storage_row)) = cursor.next() {
        let row = Row {
            row_id: rid,
            values: materialize_row(&storage_row.values, &table.schema)?,
        };

        if let Some(pred) = &del.predicate {
            if !eval_predicate(pred, &row) {
                continue;
            }
        }

        table.heap.delete(rid);
    }

    Ok(())
}

pub fn execute_update(upd: BoundUpdate, catalog: &Catalog) -> Result<(), anyhow::Error> {
    let table = catalog
        .get_table(&upd.table)
        .ok_or_else(|| anyhow::anyhow!("table '{}' not found", upd.table))?;

    let mut cursor = table.heap.scan();

    while let Some((rid, storage_row)) = cursor.next() {
        let row = Row {
            row_id: rid,
            values: materialize_row(&storage_row.values, &table.schema)?,
        };

        if let Some(pred) = &upd.predicate {
            if !eval_predicate(pred, &row) {
                continue;
            }
        }

        let mut updated = row.values.clone();
        for (col, expr) in &upd.assignments {
            let v = eval_value(expr, &row);
            updated.insert(col.name.clone(), v);
        }

        let physical = table
            .schema
            .columns
            .iter()
            .map(|c| updated.get(&c.name).cloned().unwrap_or(Value::Null))
            .collect::<Vec<_>>();

        table.heap.delete(rid);
        table.heap.insert(physical);
    }

    Ok(())
}

pub fn materialize_row(values: &[Value], schema: &Schema) -> Result<HashMap<String, Value>> {
    if values.len() != schema.columns.len() {
        bail!(
            "row/schema length mismatch: {} values, {} columns",
            values.len(),
            schema.columns.len()
        );
    }

    let mut map = HashMap::with_capacity(values.len());

    for (col, val) in schema.columns.iter().zip(values.iter()) {
        map.insert(col.name.clone(), val.clone());
    }

    Ok(map)
}
