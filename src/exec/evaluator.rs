use crate::common::value::Value;
use crate::exec::operator::Row;
use crate::ir::expr::{BinaryOp as IRBinaryOp, Expr as IRExpr, UnaryOp as IRUnaryOp};

#[derive(Debug)]
pub enum ExecError {
    DivisionByZero,
    TypeMismatch { expected: String, found: String },
    InvalidExpression(String),
}

impl std::fmt::Display for ExecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for ExecError {}

pub struct Evaluator<'a> {
    row: &'a Row,
}

impl<'a> Evaluator<'a> {
    pub fn new(row: &'a Row) -> Self {
        Self { row }
    }

    pub fn eval_expr(&self, expr: &IRExpr) -> Option<Value> {
        match expr {
            IRExpr::Literal(v) => Some(v.clone()),
            IRExpr::Null => None,

            IRExpr::BoundColumn { table, name } => {
                self.row.values.get(&format!("{table}.{name}")).cloned()
            }

            IRExpr::Column(_) => {
                panic!("BUG: AST Expr::Column reached execution");
            }

            IRExpr::Unary { op, expr } => {
                let v = self.eval_expr(expr)?;
                self.eval_unary(*op, v)
            }

            IRExpr::Binary { left, op, right } => {
                match op {
                    // arithmetic
                    IRBinaryOp::Add | IRBinaryOp::Sub | IRBinaryOp::Mul | IRBinaryOp::Div => {
                        self.eval_arithmetic(*op, left, right)
                    }

                    // logical / comparison
                    _ => self.eval_bool(expr).map(Value::Bool),
                }
            }
        }
    }

    pub fn eval_predicate(&self, expr: &IRExpr) -> bool {
        matches!(self.eval_bool(expr), Some(true))
    }

    fn eval_bool(&self, expr: &IRExpr) -> Option<bool> {
        match expr {
            IRExpr::Literal(Value::Bool(b)) => Some(*b),
            IRExpr::Literal(Value::Null) => None,

            IRExpr::Unary {
                op: IRUnaryOp::Not,
                expr,
            } => self.eval_bool(expr).map(|b| !b),

            IRExpr::Binary { left, op, right } => match op {
                IRBinaryOp::And => self.eval_and(left, right),
                IRBinaryOp::Or => self.eval_or(left, right),

                IRBinaryOp::Eq
                | IRBinaryOp::Neq
                | IRBinaryOp::Gt
                | IRBinaryOp::Gte
                | IRBinaryOp::Lt
                | IRBinaryOp::Lte => self.eval_compare(*op, left, right),

                _ => None,
            },

            _ => None,
        }
    }

    fn eval_and(&self, l: &IRExpr, r: &IRExpr) -> Option<bool> {
        match (self.eval_bool(l), self.eval_bool(r)) {
            (Some(false), _) => Some(false),
            (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        }
    }

    fn eval_or(&self, l: &IRExpr, r: &IRExpr) -> Option<bool> {
        match (self.eval_bool(l), self.eval_bool(r)) {
            (Some(true), _) => Some(true),
            (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        }
    }

    fn eval_compare(&self, op: IRBinaryOp, l: &IRExpr, r: &IRExpr) -> Option<bool> {
        let l = self.eval_expr(l)?;
        let r = self.eval_expr(r)?;

        match (op, l, r) {
            (IRBinaryOp::Eq, a, b) => Some(a == b),
            (IRBinaryOp::Neq, a, b) => Some(a != b),

            (IRBinaryOp::Gt, Value::Int64(a), Value::Int64(b)) => Some(a > b),
            (IRBinaryOp::Gte, Value::Int64(a), Value::Int64(b)) => Some(a >= b),
            (IRBinaryOp::Lt, Value::Int64(a), Value::Int64(b)) => Some(a < b),
            (IRBinaryOp::Lte, Value::Int64(a), Value::Int64(b)) => Some(a <= b),

            _ => None,
        }
    }

    fn eval_arithmetic(&self, op: IRBinaryOp, l: &IRExpr, r: &IRExpr) -> Option<Value> {
        let l = self.eval_expr(l)?;
        let r = self.eval_expr(r)?;

        match (op, l, r) {
            (IRBinaryOp::Add, Value::Int64(a), Value::Int64(b)) => Some(Value::Int64(a + b)),
            (IRBinaryOp::Sub, Value::Int64(a), Value::Int64(b)) => Some(Value::Int64(a - b)),
            (IRBinaryOp::Mul, Value::Int64(a), Value::Int64(b)) => Some(Value::Int64(a * b)),
            (IRBinaryOp::Div, Value::Int64(_), Value::Int64(0)) => None,
            (IRBinaryOp::Div, Value::Int64(a), Value::Int64(b)) => Some(Value::Int64(a / b)),
            _ => None,
        }
    }

    fn eval_unary(&self, op: IRUnaryOp, v: Value) -> Option<Value> {
        match (op, v) {
            (IRUnaryOp::Neg, Value::Int64(x)) => Some(Value::Int64(-x)),
            _ => None,
        }
    }
}
