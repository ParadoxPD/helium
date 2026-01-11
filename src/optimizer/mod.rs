pub mod cost;
pub mod errors;
pub mod rules;

use crate::{
    catalog::catalog::Catalog,
    ir::plan::LogicalPlan,
    optimizer::{
        errors::OptimizerError,
        rules::{
            constant_fold::constant_fold, index_selection::index_selection,
            predicate_pushdown::predicate_pushdown, projection_prune::projection_prune,
        },
    },
};

pub fn optimize(plan: &LogicalPlan, catalog: &Catalog) -> Result<LogicalPlan, OptimizerError> {
    let plan = constant_fold(plan)?;
    let plan = predicate_pushdown(&plan)?;
    let plan = index_selection(&plan, catalog)?;
    let plan = projection_prune(&plan)?;
    Ok(plan)
}
