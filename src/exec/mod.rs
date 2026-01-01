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
pub mod statement;
pub mod unit_tests;
pub mod update;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::api::db::QueryResult;
use crate::exec::catalog::Catalog;
use crate::exec::filter::FilterExec;
use crate::exec::index_scan::IndexScanExec;
use crate::exec::join::JoinExec;
use crate::exec::limit::LimitExec;
use crate::exec::operator::Operator;
use crate::exec::project::ProjectExec;
use crate::exec::scan::ScanExec;
use crate::exec::sort::SortExec;
use crate::frontend::sql::ast::Statement;
use crate::ir::plan::LogicalPlan;
use crate::storage::btree::node::Index;
use crate::storage::page::StorageRow;
use crate::storage::table::Table;

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
                table.heap.schema().to_vec(),
            ))
        }
    }
}

pub fn execute_statement(stmt: Statement, catalog: &mut Catalog) -> QueryResult {
    match stmt {
        Statement::CreateTable(stmt) => {
            let bound = binder.bind_create_table(stmt)?;
            catalog.create_table(bound.table, bound.schema)?;
            QueryResult::Empty
        }

        Statement::DropTable(stmt) => {
            let bound = binder.bind_drop_table(stmt)?;
            catalog.drop_table(&bound.table)?;
            QueryResult::Empty
        }

        Statement::Insert(stmt) => {
            let bound = binder.bind_insert(stmt)?;
            let table = catalog.get_table_mut(&bound.table)?;
            let mut row = StorageRow::new();
            for expr in bound.values {
                row.push(expr.eval_const()?);
            }
            table.heap.insert(row);
            QueryResult::Empty
        }

        Statement::Delete(stmt) => {
            let bound = binder.bind_delete(stmt)?;
            let plan = build_delete_plan(bound, catalog)?;
            execute_plan(plan)
        }

        Statement::Update(stmt) => {
            let bound = binder.bind_update(stmt)?;
            let plan = build_update_plan(bound, catalog)?;
            execute_plan(plan)
        }

        // SELECT and EXPLAIN go through lowering
        s @ Statement::Select(_) | s @ Statement::Explain { .. } => {
            let lowered = lower_stmt(s, catalog);
            execute_lowered(lowered, catalog)
        }

        _ => unreachable!(),
    }
}
