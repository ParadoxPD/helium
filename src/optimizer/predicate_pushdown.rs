use crate::ir::plan::{Filter, Join, LogicalPlan, Project};

pub fn predicate_pushdown(plan: &LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter(filter) => push_filter(filter),
        LogicalPlan::Project(project) => LogicalPlan::Project(Project {
            input: Box::new(predicate_pushdown(&project.input)),
            exprs: project.exprs.clone(),
        }),
        LogicalPlan::Sort(sort) => LogicalPlan::Sort(crate::ir::plan::Sort {
            input: Box::new(predicate_pushdown(&sort.input)),
            keys: sort.keys.clone(),
        }),

        LogicalPlan::Limit(limit) => LogicalPlan::Limit(limit.clone()),
        LogicalPlan::Scan(_) => plan.clone(),
        LogicalPlan::IndexScan(_) => plan.clone(),
        LogicalPlan::Join(join) => LogicalPlan::Join(Join {
            left: Box::new(predicate_pushdown(&join.left)),
            right: Box::new(predicate_pushdown(&join.right)),
            on: join.on.clone(),
        }),
    }
}

fn push_filter(filter: &Filter) -> LogicalPlan {
    let optimized_input = predicate_pushdown(&filter.input);

    match optimized_input {
        LogicalPlan::Project(project) => LogicalPlan::Project(Project {
            input: Box::new(LogicalPlan::Filter(Filter {
                input: project.input,
                predicate: filter.predicate.clone(),
            })),
            exprs: project.exprs,
        }),

        other => LogicalPlan::Filter(Filter {
            input: Box::new(other),
            predicate: filter.predicate.clone(),
        }),
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
    fn pushes_filter_below_project() {
        let plan = LogicalPlan::scan("users", "u")
            .project(vec![(Expr::bound_col("t", "name"), "name")])
            .filter(Expr::bin(
                Expr::bound_col("t", "age"),
                BinaryOp::Gt,
                Expr::lit(Value::Int64(18)),
            ));

        let optimized = predicate_pushdown(&plan);

        let expected = r#"
Project [name]
└─ Filter (age Gt 18)
   └─ Scan users
"#;

        assert_eq!(pretty(&optimized).trim(), expected.trim());
    }

    #[test]
    fn does_not_push_filter_below_limit() {
        let plan = LogicalPlan::scan("users", "u").limit(10).filter(Expr::bin(
            Expr::bound_col("t", "age"),
            BinaryOp::Gt,
            Expr::lit(Value::Int64(18)),
        ));

        let optimized = predicate_pushdown(&plan);

        // Filter must stay above Limit
        let expected = r#"
Filter (age Gt 18)
└─ Limit 10
   └─ Scan users
"#;

        assert_eq!(pretty(&optimized).trim(), expected.trim());
    }

    #[test]
    fn preserves_non_pushable_structure() {
        let plan = LogicalPlan::scan("users", "u");

        let optimized = predicate_pushdown(&plan);

        assert_eq!(pretty(&optimized), pretty(&plan));
    }

    #[test]
    fn optimizer_is_idempotent() {
        let plan = LogicalPlan::scan("users", "u")
            .project(vec![(Expr::bound_col("t", "name"), "name")])
            .filter(Expr::bin(
                Expr::bound_col("t", "age"),
                BinaryOp::Gt,
                Expr::lit(Value::Int64(18)),
            ));

        let once = predicate_pushdown(&plan);
        let twice = predicate_pushdown(&once);

        assert_eq!(pretty(&once), pretty(&twice));
    }
}
