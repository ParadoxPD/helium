pub mod expr_eval;
pub mod filter;
pub mod limit;
pub mod operator;
pub mod project;
pub mod scan;
pub mod sort;

use std::collections::HashMap;

use crate::common::value::Value;
use crate::exec::filter::FilterExec;
use crate::exec::limit::LimitExec;
use crate::exec::operator::{Operator, Row};
use crate::exec::project::ProjectExec;
use crate::exec::scan::ScanExec;
use crate::exec::sort::SortExec;
use crate::ir::plan::LogicalPlan;

pub type Catalog = HashMap<String, Vec<Row>>;

pub fn lower(plan: &LogicalPlan, catalog: &Catalog) -> Box<dyn Operator> {
    match plan {
        LogicalPlan::Scan(scan) => {
            let data = catalog.get(&scan.table).cloned().unwrap_or_default();
            Box::new(ScanExec::new(data))
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::value::Value;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::ir::plan::LogicalPlan;

    fn row(pairs: &[(&str, Value)]) -> Row {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn execute_simple_scan() {
        let mut catalog = Catalog::new();
        catalog.insert(
            "users".into(),
            vec![
                row(&[("id", Value::Int64(1))]),
                row(&[("id", Value::Int64(2))]),
            ],
        );

        let plan = LogicalPlan::scan("users");
        let mut exec = lower(&plan, &catalog);

        exec.open();
        assert!(exec.next().is_some());
        assert!(exec.next().is_some());
        assert!(exec.next().is_none());
    }

    #[test]
    fn execute_filter_project_limit() {
        let mut catalog = Catalog::new();
        catalog.insert(
            "users".into(),
            vec![
                row(&[
                    ("name", Value::String("Alice".into())),
                    ("age", Value::Int64(30)),
                ]),
                row(&[
                    ("name", Value::String("Bob".into())),
                    ("age", Value::Int64(15)),
                ]),
                row(&[
                    ("name", Value::String("Carol".into())),
                    ("age", Value::Int64(40)),
                ]),
            ],
        );

        let plan = LogicalPlan::scan("users")
            .filter(Expr::bin(
                Expr::col("age"),
                BinaryOp::Gt,
                Expr::lit(Value::Int64(18)),
            ))
            .project(vec![(Expr::col("name"), "name")])
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
        let mut catalog = Catalog::new();
        catalog.insert(
            "users".into(),
            vec![
                row(&[("x", Value::Int64(1))]),
                row(&[("x", Value::Int64(2))]),
                row(&[("x", Value::Int64(3))]),
            ],
        );

        let plan = LogicalPlan::scan("users")
            .filter(Expr::bin(
                Expr::col("x"),
                BinaryOp::Gt,
                Expr::lit(Value::Int64(1)),
            ))
            .limit(1);

        let mut exec = lower(&plan, &catalog);
        exec.open();

        let row = exec.next().unwrap();
        assert_eq!(row.get("x"), Some(&Value::Int64(2)));
        assert!(exec.next().is_none());
    }
}
