use crate::{
    catalog::catalog::Catalog,
    ir::{
        expr::{BinaryOp, Expr},
        index_predicate::IndexPredicate,
        plan::LogicalPlan,
    },
    optimizer::errors::OptimizerError,
};

pub fn index_selection(
    plan: &LogicalPlan,
    catalog: &Catalog,
) -> Result<LogicalPlan, OptimizerError> {
    Ok(match plan {
        LogicalPlan::Filter { input, predicate } => {
            let input = index_selection(input, catalog)?;

            let LogicalPlan::Scan { table_id } = &input else {
                return Ok(LogicalPlan::Filter {
                    input: Box::new(input),
                    predicate: predicate.clone(),
                });
            };

            if let Expr::Binary {
                left,
                op: BinaryOp::Eq,
                right,
            } = predicate
            {
                if let (Expr::BoundColumn { column_id, .. }, Expr::Literal(v)) = (&**left, &**right)
                {
                    if let Some(index) = catalog.find_index_on_column(*table_id, *column_id) {
                        return Ok(LogicalPlan::IndexScan {
                            table_id: *table_id,
                            index_id: index.meta.id,
                            predicate: IndexPredicate::Eq(v.clone()),
                        });
                    }
                }
            }

            LogicalPlan::Filter {
                input: Box::new(input),
                predicate: predicate.clone(),
            }
        }

        LogicalPlan::Project { input, exprs } => LogicalPlan::Project {
            input: Box::new(index_selection(input, catalog)?),
            exprs: exprs.clone(),
        },

        _ => plan.clone(),
    })
}
