pub mod expr_eval;
pub mod filter;
pub mod join;
pub mod limit;
pub mod operator;
pub mod project;
pub mod scan;
pub mod sort;

#[cfg(test)]
pub mod test_util;

use std::collections::HashMap;

use crate::exec::filter::FilterExec;
use crate::exec::join::JoinExec;
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
            let raw = catalog.get(&scan.table).unwrap();

            let rewritten = raw
                .iter()
                .map(|row| {
                    let mut out = Row::new();
                    for (k, v) in row {
                        // k is "users.name"
                        let col = k.split('.').last().unwrap();
                        out.insert(format!("{}.{}", scan.alias, col), v.clone());
                    }
                    out
                })
                .collect();

            Box::new(ScanExec::new(rewritten))
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
    use super::*;
    use crate::common::value::Value;
    use crate::exec::test_util::qrow;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::ir::plan::LogicalPlan;

    #[test]
    fn execute_simple_scan() {
        let mut catalog = Catalog::new();
        catalog.insert(
            "users".into(),
            vec![
                qrow("t", &[("id", Value::Int64(1))]),
                qrow("t", &[("id", Value::Int64(2))]),
            ],
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
        let mut catalog = Catalog::new();
        catalog.insert(
            "users".into(),
            vec![
                qrow(
                    "u",
                    &[
                        ("name", Value::String("Alice".into())),
                        ("age", Value::Int64(30)),
                    ],
                ),
                qrow(
                    "u",
                    &[
                        ("name", Value::String("Bob".into())),
                        ("age", Value::Int64(15)),
                    ],
                ),
                qrow(
                    "u",
                    &[
                        ("name", Value::String("Carol".into())),
                        ("age", Value::Int64(40)),
                    ],
                ),
            ],
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
        let mut catalog = Catalog::new();
        catalog.insert(
            "users".into(),
            vec![
                qrow("u", &[("x", Value::Int64(1))]),
                qrow("u", &[("x", Value::Int64(2))]),
                qrow("u", &[("x", Value::Int64(3))]),
            ],
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
