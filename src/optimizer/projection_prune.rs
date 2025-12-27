use std::collections::HashSet;

use crate::ir::expr::{ColumnRef, Expr};
use crate::ir::plan::{Filter, LogicalPlan, Project};

pub fn projection_prune(plan: &LogicalPlan) -> LogicalPlan {
    let mut required = HashSet::new();
    collect_required_columns(plan, &mut required);
    rewrite(plan, &mut required)
}

fn collect_required_columns(plan: &LogicalPlan, required: &mut HashSet<String>) {
    match plan {
        LogicalPlan::Project(project) => {
            for (_, alias) in &project.exprs {
                required.insert(alias.clone());
            }
            collect_required_columns(&project.input, required);
        }
        LogicalPlan::Filter(filter) => {
            collect_columns(&filter.predicate, required);
            collect_required_columns(&filter.input, required);
        }
        LogicalPlan::Sort(sort) => {
            for (expr, _) in &sort.keys {
                collect_columns(expr, required);
            }
            collect_required_columns(&sort.input, required);
        }

        LogicalPlan::Limit(limit) => {
            collect_required_columns(&limit.input, required);
        }
        LogicalPlan::Scan(_) => {}
    }
}

fn rewrite(plan: &LogicalPlan, required: &mut HashSet<String>) -> LogicalPlan {
    match plan {
        LogicalPlan::Limit(limit) => LogicalPlan::Limit(limit.clone()),

        LogicalPlan::Project(project) => {
            let kept_exprs: Vec<(Expr, String)> = project
                .exprs
                .iter()
                .filter(|(_, alias)| required.contains(alias))
                .cloned()
                .collect();

            let rewritten_input = rewrite(&project.input, required);

            if let LogicalPlan::Scan(mut scan) = rewritten_input {
                let mut projected_cols = Vec::new();
                let mut is_identity = true;

                for (expr, alias) in &kept_exprs {
                    match expr {
                        Expr::Column(col) if col.name == *alias => {
                            projected_cols.push(alias.clone());
                        }
                        _ => {
                            is_identity = false;
                            break;
                        }
                    }
                }

                if is_identity {
                    scan.columns = projected_cols;
                    return LogicalPlan::Scan(scan);
                }

                LogicalPlan::Project(Project {
                    input: Box::new(LogicalPlan::Scan(scan)),
                    exprs: kept_exprs,
                })
            } else {
                LogicalPlan::Project(Project {
                    input: Box::new(rewritten_input),
                    exprs: kept_exprs,
                })
            }
        }

        LogicalPlan::Filter(filter) => LogicalPlan::Filter(Filter {
            input: Box::new(rewrite(&filter.input, required)),
            predicate: filter.predicate.clone(),
        }),

        LogicalPlan::Scan(scan) => {
            let mut scan = scan.clone();
            scan.columns = required.iter().cloned().collect();
            LogicalPlan::Scan(scan)
        }

        LogicalPlan::Sort(sort) => LogicalPlan::Sort(crate::ir::plan::Sort {
            input: Box::new(rewrite(&sort.input, required)),
            keys: sort.keys.clone(),
        }),
    }
}

fn collect_columns(expr: &Expr, required: &mut HashSet<String>) {
    match expr {
        Expr::Column(ColumnRef { name, .. }) => {
            required.insert(name.clone());
        }
        Expr::Unary { expr, .. } => collect_columns(expr, required),
        Expr::Binary { left, right, .. } => {
            collect_columns(left, required);
            collect_columns(right, required);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::value::Value;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::ir::plan::LogicalPlan;
    use crate::ir::pretty::pretty;

    #[test]
    fn prunes_unused_project_fields() {
        let plan = LogicalPlan::scan("users")
            .project(vec![
                (Expr::col("name"), "name"),
                (Expr::col("city"), "city"),
                (Expr::col("age"), "age"),
            ])
            .filter(Expr::bin(
                Expr::col("age"),
                BinaryOp::Gt,
                Expr::lit(Value::Int64(18)),
            ))
            .project(vec![
                (Expr::col("name"), "name"),
                (Expr::col("city"), "city"),
            ]);

        let optimized = projection_prune(&plan);

        let expected = r#"
Project [name, city]
└─ Filter (age Gt 18)
   └─ Scan users
"#;

        assert_eq!(pretty(&optimized).trim(), expected.trim());
    }

    #[test]
    fn keeps_columns_used_in_filter() {
        let plan = LogicalPlan::scan("users")
            .project(vec![(Expr::col("name"), "name"), (Expr::col("age"), "age")])
            .filter(Expr::bin(
                Expr::col("age"),
                BinaryOp::Gt,
                Expr::lit(Value::Int64(30)),
            ))
            .project(vec![(Expr::col("name"), "name")]);

        let optimized = projection_prune(&plan);

        let output = pretty(&optimized);

        assert!(output.contains("Scan users"));
        assert!(output.contains("age"));
    }

    #[test]
    fn idempotent_projection_prune() {
        let plan = LogicalPlan::scan("users")
            .project(vec![(Expr::col("name"), "name"), (Expr::col("age"), "age")]);

        let once = projection_prune(&plan);
        let twice = projection_prune(&once);

        assert_eq!(pretty(&once), pretty(&twice));
    }
}
