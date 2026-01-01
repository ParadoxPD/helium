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
