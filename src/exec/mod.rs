pub mod expr_eval;
pub mod filter;
pub mod join;
pub mod limit;
pub mod operator;
pub mod project;
pub mod scan;
pub mod sort;

use std::collections::HashMap;
use std::sync::Arc;

use crate::exec::filter::FilterExec;
use crate::exec::join::JoinExec;
use crate::exec::limit::LimitExec;
use crate::exec::operator::{Operator, Row};
use crate::exec::project::ProjectExec;
use crate::exec::scan::ScanExec;
use crate::exec::sort::SortExec;
use crate::ir::plan::LogicalPlan;
use crate::storage::table::Table;

pub type Catalog = HashMap<String, Arc<dyn Table>>;

pub fn lower(plan: &LogicalPlan, catalog: &Catalog) -> Box<dyn Operator> {
    match plan {
        LogicalPlan::Scan(scan) => {
            let table = catalog.get(&scan.table).expect("table not found");
            let scan_exec = ScanExec::new(table.clone(), scan.alias.clone(), scan.columns.clone());
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
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::common::value::Value;
    use crate::exec::operator::Operator;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::ir::plan::LogicalPlan;
    use crate::storage::in_memory::InMemoryTable;
    use crate::storage::page::{PageId, RowId, StorageRow};

    fn srow(slot: u16, values: Vec<Value>) -> StorageRow {
        StorageRow {
            rid: RowId {
                page_id: PageId(0),
                slot_id: slot,
            },
            values,
        }
    }

    #[test]
    fn execute_simple_scan() {
        let schema = vec!["id confirmed".into(), "name".into(), "age".into()];

        let rows = vec![
            srow(0, vec![Value::Int64(1), Value::Null, Value::Null]),
            srow(1, vec![Value::Int64(2), Value::Null, Value::Null]),
        ];

        let mut catalog = Catalog::new();
        catalog.insert(
            "users".into(),
            Arc::new(InMemoryTable::new("users".into(), schema.clone(), rows)),
        );

        let plan = LogicalPlan::scan("users", "u");
        let mut exec = lower(&plan, &catalog);

        exec.open();
        assert!(exec.next().is_some());
        assert!(exec.next().is_some());
        assert!(exec.next().is_none());
    }

    #[test]
    fn execute_filter_project_limit() {
        let schema = vec!["name".into(), "age".into()];

        let rows = vec![
            srow(0, vec![Value::String("Alice".into()), Value::Int64(30)]),
            srow(1, vec![Value::String("Bob".into()), Value::Int64(15)]),
            srow(2, vec![Value::String("Carol".into()), Value::Int64(40)]),
        ];

        let mut catalog = Catalog::new();
        catalog.insert(
            "users".into(),
            Arc::new(InMemoryTable::new("users".into(), schema.clone(), rows)),
        );

        let plan = LogicalPlan::scan("users", "u")
            .filter(Expr::bin(
                Expr::bound_col("u", "age"),
                BinaryOp::Gt,
                Expr::lit(Value::Int64(18)),
            ))
            .project(vec![(Expr::bound_col("u", "name"), "name")])
            .limit(2);

        let mut exec = lower(&plan, &catalog);
        exec.open();

        let r1 = exec.next().unwrap();
        let r2 = exec.next().unwrap();

        assert_eq!(r1.get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(r2.get("name"), Some(&Value::String("Carol".into())));
        assert!(exec.next().is_none());
    }

    #[test]
    fn execution_respects_optimizer_output() {
        let schema = vec!["x".into()];

        let rows = vec![
            srow(0, vec![Value::Int64(1)]),
            srow(1, vec![Value::Int64(2)]),
            srow(2, vec![Value::Int64(3)]),
        ];

        let mut catalog = Catalog::new();
        catalog.insert(
            "users".into(),
            Arc::new(InMemoryTable::new("users".into(), schema.clone(), rows)),
        );

        let plan = LogicalPlan::scan("users", "u")
            .filter(Expr::bin(
                Expr::bound_col("u", "x"),
                BinaryOp::Gt,
                Expr::lit(Value::Int64(1)),
            ))
            .limit(1);

        let mut exec = lower(&plan, &catalog);
        exec.open();

        let row = exec.next().unwrap();
        assert_eq!(row.get("u.x"), Some(&Value::Int64(2)));
        assert!(exec.next().is_none());
    }
}
