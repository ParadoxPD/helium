use crate::ir::expr::Expr;
use crate::ir::plan::{Filter, Limit, LogicalPlan, Project, Scan};

#[derive(Debug, PartialEq)]
pub enum ValidationError {
    EmptyProject,
    ZeroLimit,
    NullPredicate,
    InvalidStructure(&'static str),
}

pub type ValidationResult = Result<(), ValidationError>;

pub fn validate(plan: &LogicalPlan) -> ValidationResult {
    validate_node(plan)
}

fn validate_node(plan: &LogicalPlan) -> ValidationResult {
    match plan {
        LogicalPlan::Scan(scan) => validate_scan(scan),
        LogicalPlan::Filter(filter) => validate_filter(filter),
        LogicalPlan::Project(project) => validate_project(project),
        LogicalPlan::Limit(limit) => validate_limit(limit),
    }
}

fn validate_scan(_scan: &Scan) -> ValidationResult {
    Ok(())
}

fn validate_filter(filter: &Filter) -> ValidationResult {
    if matches!(filter.predicate, Expr::Null) {
        return Err(ValidationError::NullPredicate);
    }
    validate_node(&filter.input)
}

fn validate_project(project: &Project) -> ValidationResult {
    if project.exprs.is_empty() {
        return Err(ValidationError::EmptyProject);
    }

    validate_node(&project.input)
}

fn validate_limit(limit: &Limit) -> ValidationResult {
    if limit.count == 0 {
        return Err(ValidationError::ZeroLimit);
    }

    validate_node(&limit.input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::value::Value;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::ir::plan::LogicalPlan;

    #[test]
    fn valid_simple_scan() {
        let plan = LogicalPlan::scan("users");
        assert_eq!(validate(&plan), Ok(()));
    }

    #[test]
    fn valid_filter_plan() {
        let plan = LogicalPlan::scan("users").filter(Expr::bin(
            Expr::col("age"),
            BinaryOp::Gt,
            Expr::lit(Value::Int64(18)),
        ));

        assert_eq!(validate(&plan), Ok(()));
    }

    #[test]
    fn project_must_not_be_empty() {
        let plan = LogicalPlan::Project(crate::ir::plan::Project {
            input: Box::new(LogicalPlan::scan("users")),
            exprs: vec![],
        });

        assert_eq!(validate(&plan), Err(ValidationError::EmptyProject));
    }

    #[test]
    fn limit_must_be_positive() {
        let plan = LogicalPlan::scan("users").limit(0);

        assert_eq!(validate(&plan), Err(ValidationError::ZeroLimit));
    }

    #[test]
    fn filter_predicate_cannot_be_null() {
        let plan = LogicalPlan::scan("users").filter(Expr::Null);

        assert_eq!(validate(&plan), Err(ValidationError::NullPredicate));
    }

    #[test]
    fn deeply_nested_plan_validates() {
        let plan = LogicalPlan::scan("users")
            .filter(Expr::bin(
                Expr::col("active"),
                BinaryOp::Eq,
                Expr::lit(Value::Bool(true)),
            ))
            .project(vec![(Expr::col("email"), "email")])
            .limit(10);

        assert_eq!(validate(&plan), Ok(()));
    }
}
