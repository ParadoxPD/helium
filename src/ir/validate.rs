use crate::ir::expr::Expr;
use crate::ir::plan::{Filter, Join, Limit, LogicalPlan, Project, Scan, Sort};

#[derive(Debug, PartialEq)]
pub enum ValidationError {
    EmptyProject,
    ZeroLimit,
    NullPredicate,
    InvalidStructure(&'static str),
    EmptySortKeys,
}

pub type ValidationResult = Result<(), ValidationError>;

pub fn validate(plan: &LogicalPlan) -> ValidationResult {
    validate_node(plan)
}

fn validate_node(plan: &LogicalPlan) -> ValidationResult {
    match plan {
        LogicalPlan::Scan(scan) => validate_scan(scan),
        LogicalPlan::IndexScan(_) => Ok(()),
        LogicalPlan::Sort(sort) => validate_sort(sort),
        LogicalPlan::Filter(filter) => validate_filter(filter),
        LogicalPlan::Project(project) => validate_project(project),
        LogicalPlan::Limit(limit) => validate_limit(limit),
        LogicalPlan::Join(join) => validate_join(join),
    }
}

fn validate_scan(_scan: &Scan) -> ValidationResult {
    Ok(())
}

fn validate_join(join: &Join) -> ValidationResult {
    validate_node(&join.left)?;
    validate_node(&join.right)?;
    Ok(())
}

fn validate_sort(sort: &Sort) -> ValidationResult {
    if sort.keys.is_empty() {
        return Err(ValidationError::EmptySortKeys);
    }
    validate_node(&sort.input)
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
        let plan = LogicalPlan::scan("users", "u");
        assert_eq!(validate(&plan), Ok(()));
    }

    #[test]
    fn valid_filter_plan() {
        let plan = LogicalPlan::scan("users", "u").filter(Expr::bin(
            Expr::bound_col("t", "age"),
            BinaryOp::Gt,
            Expr::lit(Value::Int64(18)),
        ));

        assert_eq!(validate(&plan), Ok(()));
    }

    #[test]
    fn project_must_not_be_empty() {
        let plan = LogicalPlan::Project(crate::ir::plan::Project {
            input: Box::new(LogicalPlan::scan("users", "u")),
            exprs: vec![],
        });

        assert_eq!(validate(&plan), Err(ValidationError::EmptyProject));
    }

    #[test]
    fn limit_must_be_positive() {
        let plan = LogicalPlan::scan("users", "u").limit(0);

        assert_eq!(validate(&plan), Err(ValidationError::ZeroLimit));
    }

    #[test]
    fn filter_predicate_cannot_be_null() {
        let plan = LogicalPlan::scan("users", "u").filter(Expr::Null);

        assert_eq!(validate(&plan), Err(ValidationError::NullPredicate));
    }

    #[test]
    fn deeply_nested_plan_validates() {
        let plan = LogicalPlan::scan("users", "u")
            .filter(Expr::bin(
                Expr::bound_col("t", "active"),
                BinaryOp::Eq,
                Expr::lit(Value::Bool(true)),
            ))
            .project(vec![(Expr::bound_col("t", "email"), "email")])
            .limit(10);

        assert_eq!(validate(&plan), Ok(()));
    }
}
