use std::fmt;

use crate::common::types::DataType;
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
    Le,
    Gt,
    Ge,

    And,
    Or,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Column(ColumnRef),

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

    Null,
}

impl Expr {
    pub fn col(name: impl Into<String>) -> Self {
        Expr::Column(ColumnRef::unresolved(name))
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::value::Value;

    #[test]
    fn literal_is_constant() {
        let expr = Expr::lit(Value::Int64(42));
        assert!(expr.is_constant());
    }

    #[test]
    fn column_is_not_constant() {
        let expr = Expr::col("age");
        assert!(!expr.is_constant());
    }

    #[test]
    fn binary_constant_expression() {
        let expr = Expr::bin(
            Expr::lit(Value::Int64(10)),
            BinaryOp::Add,
            Expr::lit(Value::Int64(20)),
        );

        assert!(expr.is_constant());
    }

    #[test]
    fn binary_mixed_expression_not_constant() {
        let expr = Expr::bin(
            Expr::col("salary"),
            BinaryOp::Gt,
            Expr::lit(Value::Int64(1000)),
        );

        assert!(!expr.is_constant());
    }

    #[test]
    fn unary_expression_constant_propagation() {
        let expr = Expr::unary(UnaryOp::Neg, Expr::lit(Value::Int64(5)));

        assert!(expr.is_constant());
    }

    #[test]
    fn column_ref_unresolved() {
        let col = ColumnRef::unresolved("name");

        assert_eq!(col.name, "name");
        assert!(col.table.is_none());
        assert!(col.index.is_none());
    }

    #[test]
    fn column_ref_resolved() {
        let col = ColumnRef::resolved("name", 3);

        assert_eq!(col.name, "name");
        assert_eq!(col.index, Some(3));
    }

    #[test]
    fn display_simple_binary_expr() {
        let expr = Expr::bin(Expr::col("age"), BinaryOp::Gt, Expr::lit(Value::Int64(18)));

        let printed = format!("{expr}");
        assert!(printed.contains("age"));
        assert!(printed.contains("Gt"));
        assert!(printed.contains("18"));
    }

    #[test]
    fn nested_expression_structure() {
        let expr = Expr::bin(
            Expr::bin(Expr::col("a"), BinaryOp::Add, Expr::lit(Value::Int64(1))),
            BinaryOp::Mul,
            Expr::lit(Value::Int64(2)),
        );

        // Structural equality check
        match expr {
            Expr::Binary { left, op, right } => {
                assert_eq!(op, BinaryOp::Mul);
                assert!(right.is_constant());

                match *left {
                    Expr::Binary { op: inner_op, .. } => {
                        assert_eq!(inner_op, BinaryOp::Add);
                    }
                    _ => panic!("expected nested binary expression"),
                }
            }
            _ => panic!("expected binary expression"),
        }
    }
}
