use crate::ir::plan::{Filter, Limit, LogicalPlan, Project, Scan};

/// Pretty-print a logical plan into a tree-like string.
pub fn pretty(plan: &LogicalPlan) -> String {
    let mut out = String::new();
    fmt_plan(plan, "", true, true, &mut out);
    out
}

fn fmt_plan(plan: &LogicalPlan, prefix: &str, is_last: bool, is_root: bool, out: &mut String) {
    if is_root {
        out.push_str(&node_label(plan));
        out.push('\n');
    } else {
        out.push_str(prefix);
        out.push_str(if is_last { "└─ " } else { "├─ " });
        out.push_str(&node_label(plan));
        out.push('\n');
    }

    let child_prefix = if is_root {
        String::new()
    } else if is_last {
        format!("{prefix}   ")
    } else {
        format!("{prefix}│  ")
    };

    match plan {
        LogicalPlan::Scan(_) => {}
        LogicalPlan::Filter(Filter { input, .. }) => {
            fmt_plan(input, &child_prefix, true, false, out);
        }
        LogicalPlan::Project(Project { input, .. }) => {
            fmt_plan(input, &child_prefix, true, false, out);
        }
        LogicalPlan::Limit(Limit { input, .. }) => {
            fmt_plan(input, &child_prefix, true, false, out);
        }
    }
}

fn node_label(plan: &LogicalPlan) -> String {
    match plan {
        LogicalPlan::Scan(Scan { table, .. }) => {
            format!("Scan {table}")
        }
        LogicalPlan::Filter(Filter { predicate, .. }) => {
            // predicate already formats nicely
            format!("Filter {predicate}")
        }
        LogicalPlan::Project(Project { exprs, .. }) => {
            let fields = exprs
                .iter()
                .map(|(_, alias)| alias.as_str())
                .collect::<Vec<_>>()
                .join(", ");

            format!("Project [{fields}]")
        }
        LogicalPlan::Limit(Limit { count, .. }) => {
            format!("Limit {count}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::value::Value;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::ir::plan::LogicalPlan;

    #[test]
    fn pretty_print_scan() {
        let plan = LogicalPlan::scan("users");

        let output = pretty(&plan);

        assert_eq!(output.trim(), "Scan users");
    }

    #[test]
    fn pretty_print_filter() {
        let plan = LogicalPlan::scan("users").filter(Expr::bin(
            Expr::col("age"),
            BinaryOp::Gt,
            Expr::lit(Value::Int64(18)),
        ));

        let output = pretty(&plan);

        assert!(output.contains("Filter"));
        assert!(output.contains("Scan users"));
    }

    #[test]
    fn pretty_print_project_chain() {
        let plan = LogicalPlan::scan("users")
            .filter(Expr::bin(
                Expr::col("active"),
                BinaryOp::Eq,
                Expr::lit(Value::Bool(true)),
            ))
            .project(vec![
                (Expr::col("email"), "email"),
                (Expr::col("name"), "name"),
            ])
            .limit(5);

        let output = pretty(&plan);

        let expected = r#"
Limit 5
└─ Project [email, name]
   └─ Filter (active Eq true)
      └─ Scan users
"#;

        assert_eq!(output.trim(), expected.trim());
    }

    #[test]
    fn pretty_output_is_stable() {
        let plan = LogicalPlan::scan("users").limit(1);

        let a = pretty(&plan);
        let b = pretty(&plan);

        assert_eq!(a, b);
    }
}
