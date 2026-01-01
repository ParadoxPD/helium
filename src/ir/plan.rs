use crate::{common::value::Value, ir::expr::Expr};

#[derive(Clone, Debug, PartialEq)]
pub enum LogicalPlan {
    Scan(Scan),
    Filter(Filter),
    Project(Project),
    Sort(Sort),
    Limit(Limit),
    Join(Join),
    IndexScan(IndexScan),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Scan {
    pub table: String,
    pub alias: String,
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

#[derive(Clone, Debug, PartialEq)]
pub struct Join {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub on: Expr,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IndexScan {
    pub table: String,
    pub column: String,
    pub predicate: IndexPredicate,
}

#[derive(Clone, Debug, PartialEq)]
pub enum IndexPredicate {
    Eq(Value),
    Range { low: Value, high: Value },
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
    pub fn scan(table: impl Into<String>, alias: impl Into<String>) -> Self {
        LogicalPlan::Scan(Scan {
            table: table.into(),
            alias: alias.into(),
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
            LogicalPlan::IndexScan(_) => 0,
            LogicalPlan::Filter(_) => 1,
            LogicalPlan::Project(_) => 1,
            LogicalPlan::Sort(_) => 1,
            LogicalPlan::Limit(_) => 1,
            LogicalPlan::Join(_) => 2,
        }
    }

    pub fn input(&self) -> Option<&LogicalPlan> {
        match self {
            LogicalPlan::Filter(f) => Some(&f.input),
            LogicalPlan::Project(p) => Some(&p.input),
            LogicalPlan::Limit(l) => Some(&l.input),
            LogicalPlan::Sort(s) => Some(&s.input),
            LogicalPlan::Join(_) => None,
            LogicalPlan::Scan(_) => None,
            LogicalPlan::IndexScan(_) => None,
        }
    }
}
