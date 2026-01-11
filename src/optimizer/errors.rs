use std::fmt;

#[derive(Debug)]
pub enum OptimizerError {
    InvalidPlan { reason: String },
    UnsupportedRule { rule: &'static str },
    CatalogError { message: String },
}

impl fmt::Display for OptimizerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPlan { reason } => write!(f, "optimizer error: invalid plan ({})", reason),
            Self::UnsupportedRule { rule } => {
                write!(f, "optimizer error: unsupported rule '{}'", rule)
            }
            Self::CatalogError { message } => write!(f, "optimizer error: {}", message),
        }
    }
}

impl std::error::Error for OptimizerError {}
