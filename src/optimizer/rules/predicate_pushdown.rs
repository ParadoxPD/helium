use crate::{ir::plan::LogicalPlan, optimizer::errors::OptimizerError};

pub fn predicate_pushdown(plan: &LogicalPlan) -> Result<LogicalPlan, OptimizerError> {
    Ok(match plan {
        LogicalPlan::Filter { input, predicate } => {
            let optimized_input = predicate_pushdown(input)?;

            match optimized_input {
                LogicalPlan::Project { input, exprs } => LogicalPlan::Project {
                    input: Box::new(LogicalPlan::Filter {
                        input,
                        predicate: predicate.clone(),
                    }),
                    exprs,
                },
                other => LogicalPlan::Filter {
                    input: Box::new(other),
                    predicate: predicate.clone(),
                },
            }
        }

        LogicalPlan::Project { input, exprs } => LogicalPlan::Project {
            input: Box::new(predicate_pushdown(input)?),
            exprs: exprs.clone(),
        },

        LogicalPlan::Sort { input, keys } => LogicalPlan::Sort {
            input: Box::new(predicate_pushdown(input)?),
            keys: keys.clone(),
        },

        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
        } => LogicalPlan::Join {
            left: Box::new(predicate_pushdown(left)?),
            right: Box::new(predicate_pushdown(right)?),
            on: on.clone(),
            join_type: join_type.clone(),
        },

        _ => plan.clone(),
    })
}
