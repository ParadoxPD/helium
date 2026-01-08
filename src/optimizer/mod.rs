pub fn optimize(plan: &LogicalPlan, catalog: &Catalog) -> LogicalPlan {
    let plan = constant_fold(plan);
    let plan = predicate_pushdown(&plan);
    let plan = index_selection(&plan, catalog);
    let plan = projection_prune(&plan);
    plan
}
