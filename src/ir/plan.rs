use crate::ir::expr::Expr;

#[derive(Clone, Debug, PartialEq)]
pub enum LogicalPlan {
    Scan(Scan),
    Filter(Filter),
    Project(Project),
    Sort(Sort),
    Limit(Limit),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Scan {
    pub table: String,
    pub columns: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Filter {
    pub input: Box<LogicalPlan>,
    pub predicate: Expr,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Project {
    pub input: Box<LogicalPlan>,
    pub exprs: Vec<(Expr, String)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Limit {
    pub input: Box<LogicalPlan>,
    pub count: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Sort {
    pub input: Box<LogicalPlan>,
    pub keys: Vec<(Expr, bool)>, // (expr, asc)
}

impl Limit {
    pub fn clone_with_input(&self, input: LogicalPlan) -> Self {
        Self {
            count: self.count,
            input: Box::new(input),
        }
    }
}

impl LogicalPlan {
    pub fn scan(table: impl Into<String>) -> Self {
        LogicalPlan::Scan(Scan {
            table: table.into(),
            columns: Vec::new(),
        })
    }

    pub fn filter(self, predicate: Expr) -> Self {
        LogicalPlan::Filter(Filter {
            input: Box::new(self),
            predicate,
        })
    }

    pub fn project(self, exprs: Vec<(Expr, impl Into<String>)>) -> Self {
        LogicalPlan::Project(Project {
            input: Box::new(self),
            exprs: exprs.into_iter().map(|(e, a)| (e, a.into())).collect(),
        })
    }

    pub fn sort(self, keys: Vec<(Expr, bool)>) -> Self {
        LogicalPlan::Sort(Sort {
            input: Box::new(self),
            keys,
        })
    }

    pub fn limit(self, count: usize) -> Self {
        LogicalPlan::Limit(Limit {
            input: Box::new(self),
            count,
        })
    }

    pub fn arity(&self) -> usize {
        match self {
            LogicalPlan::Scan(_) => 0,
            LogicalPlan::Filter(_) => 1,
            LogicalPlan::Project(_) => 1,
            LogicalPlan::Sort(_) => 1,
            LogicalPlan::Limit(_) => 1,
        }
    }

    pub fn input(&self) -> Option<&LogicalPlan> {
        match self {
            LogicalPlan::Filter(f) => Some(&f.input),
            LogicalPlan::Project(p) => Some(&p.input),
            LogicalPlan::Limit(l) => Some(&l.input),
            LogicalPlan::Sort(s) => Some(&s.input),
            LogicalPlan::Scan(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::value::Value;
    use crate::ir::expr::{BinaryOp, Expr};

    #[test]
    fn scan_has_no_children() {
        let plan = LogicalPlan::scan("users");

        assert_eq!(plan.arity(), 0);
        assert!(plan.input().is_none());
    }

    #[test]
    fn filter_wraps_input() {
        let plan = LogicalPlan::scan("users").filter(Expr::bin(
            Expr::col("age"),
            BinaryOp::Gt,
            Expr::lit(Value::Int64(18)),
        ));

        assert_eq!(plan.arity(), 1);
        assert!(plan.input().is_some());
    }

    #[test]
    fn project_preserves_structure() {
        let plan = LogicalPlan::scan("users").project(vec![
            (Expr::col("name"), "name"),
            (Expr::col("city"), "city"),
        ]);

        match plan {
            LogicalPlan::Project(p) => {
                assert_eq!(p.exprs.len(), 2);
                assert_eq!(p.input.arity(), 0);
            }
            _ => panic!("expected Project node"),
        }
    }

    #[test]
    fn limit_wraps_project() {
        let plan = LogicalPlan::scan("users")
            .project(vec![(Expr::col("name"), "name")])
            .limit(10);

        match plan {
            LogicalPlan::Limit(l) => {
                assert_eq!(l.count, 10);

                match *l.input {
                    LogicalPlan::Project(_) => {}
                    _ => panic!("expected Project under Limit"),
                }
            }
            _ => panic!("expected Limit node"),
        }
    }

    #[test]
    fn chaining_builds_correct_tree_shape() {
        let plan = LogicalPlan::scan("users")
            .filter(Expr::bin(
                Expr::col("active"),
                BinaryOp::Eq,
                Expr::lit(Value::Bool(true)),
            ))
            .project(vec![(Expr::col("email"), "email")])
            .limit(5);

        // Limit
        let limit = match plan {
            LogicalPlan::Limit(l) => l,
            _ => panic!("expected Limit"),
        };

        // Project
        let project = match *limit.input {
            LogicalPlan::Project(p) => p,
            _ => panic!("expected Project"),
        };

        // Filter
        let filter = match *project.input {
            LogicalPlan::Filter(f) => f,
            _ => panic!("expected Filter"),
        };

        // Scan
        match *filter.input {
            LogicalPlan::Scan(_) => {}
            _ => panic!("expected Scan"),
        }
    }
}
