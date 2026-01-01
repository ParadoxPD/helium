use std::fmt;

use crate::common::value::Value;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub name: String,
    pub index: Option<usize>,
}

impl ColumnRef {
    pub fn unresolved(name: impl Into<String>) -> Self {
        Self {
            table: None,
            name: name.into(),
            index: None,
        }
    }

    pub fn resolved(name: impl Into<String>, index: usize) -> Self {
        Self {
            table: None,
            name: name.into(),
            index: Some(index),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,

    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,

    And,
    Or,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Column(ColumnRef),

    Literal(Value),

    BoundColumn {
        table: String,
        name: String,
    },

    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },

    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },

    Null,
}

impl Expr {
    pub fn col(name: impl Into<String>) -> Self {
        Expr::Column(ColumnRef::unresolved(name))
    }

    pub fn bound_col(table: impl Into<String>, name: impl Into<String>) -> Self {
        Expr::BoundColumn {
            table: table.into(),
            name: name.into(),
        }
    }

    pub fn lit(value: Value) -> Self {
        Expr::Literal(value)
    }

    pub fn bin(left: Expr, op: BinaryOp, right: Expr) -> Self {
        Expr::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    pub fn unary(op: UnaryOp, expr: Expr) -> Self {
        Expr::Unary {
            op,
            expr: Box::new(expr),
        }
    }

    pub fn is_constant(&self) -> bool {
        match self {
            Expr::Literal(_) | Expr::Null => true,
            Expr::Column(_) => false,
            Expr::BoundColumn { .. } => false,
            Expr::Unary { expr, .. } => expr.is_constant(),
            Expr::Binary { left, right, .. } => left.is_constant() && right.is_constant(),
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Column(c) => {
                if let Some(table) = &c.table {
                    write!(f, "{}.{}", table, c.name)
                } else {
                    write!(f, "{}", c.name)
                }
            }
            Expr::Literal(v) => write!(f, "{v}"),
            Expr::Null => write!(f, "NULL"),
            Expr::Unary { op, expr } => {
                write!(f, "{:?} {}", op, expr)
            }
            Expr::Binary { left, op, right } => {
                write!(f, "{} {:?} {}", left, op, right)
            }
            Expr::BoundColumn { name, .. } => {
                write!(f, "{}", name)
            }
        }
    }
}
