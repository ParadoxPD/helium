//! Logical expressions used in IR.
//!
//! This module is FROZEN.
//! Expressions here are fully bound and resolved.

use crate::types::schema::ColumnId;
use crate::types::value::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UnaryOp {
    Not,
    Neg,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
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
#[non_exhaustive]
pub enum Expr {
    /// Reference to a resolved column
    Column {
        column_id: ColumnId,
    },

    /// Literal runtime value
    Literal(Value),

    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },

    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },

    /// Explicit NULL literal
    Null,
}
