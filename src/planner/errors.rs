use std::fmt;

use crate::ir::plan::LogicalPlan;

pub type PlanResult = Result<LogicalPlan, PlanError>;

#[derive(Debug)]
pub enum PlanError {
    InvalidPlan { reason: &'static str },

    UnsupportedFeature { feature: &'static str },

    InvalidPredicate { message: String },

    InvalidJoin { message: String },
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlanError::InvalidPlan { reason } => {
                write!(f, "planner error: invalid plan ({})", reason)
            }

            PlanError::UnsupportedFeature { feature } => {
                write!(f, "planner error: unsupported feature '{}'", feature)
            }

            PlanError::InvalidPredicate { message } => {
                write!(f, "planner error: invalid predicate ({})", message)
            }

            PlanError::InvalidJoin { message } => {
                write!(f, "planner error: invalid join ({})", message)
            }
        }
    }
}

impl std::error::Error for PlanError {}
