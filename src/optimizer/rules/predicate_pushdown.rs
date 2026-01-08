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
