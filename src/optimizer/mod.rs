pub mod constant_fold;
pub mod index_selection;
pub mod predicate_pushdown;
pub mod projection_prune;

use crate::exec::Catalog;
use crate::ir::expr::{BinaryOp, Expr};
use crate::ir::plan::{IndexScan, LogicalPlan};

use crate::optimizer::{
    constant_fold::constant_fold, index_selection::index_selection,
    predicate_pushdown::predicate_pushdown, projection_prune::projection_prune,
};

pub fn optimize(plan: &LogicalPlan, catalog: &Catalog) -> LogicalPlan {
    let plan = constant_fold(plan);
    let plan = predicate_pushdown(&plan);
    let plan = index_selection(&plan, catalog);
    let plan = projection_prune(&plan);
    plan
}
