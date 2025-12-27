pub mod constant_fold;
pub mod predicate_pushdown;
pub mod projection_prune;

use crate::ir::plan::LogicalPlan;

use crate::optimizer::{
    constant_fold::constant_fold, predicate_pushdown::predicate_pushdown,
    projection_prune::projection_prune,
};

pub fn optimize(plan: &LogicalPlan) -> LogicalPlan {
    let plan = constant_fold(plan);
    let plan = predicate_pushdown(&plan);
    let plan = projection_prune(&plan);
    plan
}
