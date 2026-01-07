use crate::common::value::Value;
use crate::exec::operator::Row;
use crate::ir::expr::{BinaryOp as IRBinaryOp, Expr as IRExpr, UnaryOp as IRUnaryOp};

#[derive(Debug)]
pub enum ExecError {
    DivisionByZero,
    TypeMismatch {
        op: String,
        left: Value,
        right: Option<Value>,
    },
    InvalidUnary {
        op: String,
        value: Value,
    },
    NonBooleanPredicate(Value),
}

pub type EvalResult<T> = Result<Option<T>, ExecError>;

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

    pub fn eval_expr(&self, expr: &IRExpr) -> EvalResult<Value> {
        match expr {
            IRExpr::Literal(v) => Ok(Some(v.clone())),
            IRExpr::Null => Ok(None),

            IRExpr::BoundColumn { table, name } => {
                Ok(self.row.values.get(&format!("{table}.{name}")).cloned())
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
                    _ => {
                        let b = self.eval_bool(expr)?;
                        Ok(b.map(Value::Bool))
                    }
                }
            }
        }
    }

    pub fn eval_predicate(&self, expr: &IRExpr) -> Result<bool, ExecError> {
        match self.eval_bool(expr)? {
            Some(true) => Ok(true),
            Some(false) | None => Ok(false), // SQL semantics
        }
    }
    fn eval_bool(&self, expr: &IRExpr) -> EvalResult<bool> {
        match expr {
            IRExpr::Literal(Value::Bool(b)) => Ok(Some(*b)),
            IRExpr::Literal(Value::Null) => Ok(None),

            IRExpr::Unary {
                op: IRUnaryOp::Not,
                expr,
            } => match self.eval_bool(expr)? {
                Some(b) => Ok(Some(!b)),
                None => Ok(None),
            },

            IRExpr::Binary { left, op, right } => match op {
                IRBinaryOp::And => self.eval_and(left, right),
                IRBinaryOp::Or => self.eval_or(left, right),

                IRBinaryOp::Eq
                | IRBinaryOp::Neq
                | IRBinaryOp::Gt
                | IRBinaryOp::Gte
                | IRBinaryOp::Lt
                | IRBinaryOp::Lte => self.eval_compare(*op, left, right),

                _ => Err(ExecError::NonBooleanPredicate(
                    self.eval_expr(expr)?.unwrap_or(Value::Null),
                )),
            },

            _ => Err(ExecError::NonBooleanPredicate(
                self.eval_expr(expr)?.unwrap_or(Value::Null),
            )),
        }
    }

    fn eval_and(&self, l: &IRExpr, r: &IRExpr) -> EvalResult<bool> {
        match (self.eval_bool(l)?, self.eval_bool(r)?) {
            (Some(false), _) => Ok(Some(false)),
            (_, Some(false)) => Ok(Some(false)),
            (Some(true), Some(true)) => Ok(Some(true)),
            _ => Ok(None),
        }
    }

    fn eval_or(&self, l: &IRExpr, r: &IRExpr) -> EvalResult<bool> {
        match (self.eval_bool(l)?, self.eval_bool(r)?) {
            (Some(true), _) => Ok(Some(true)),
            (_, Some(true)) => Ok(Some(true)),
            (Some(false), Some(false)) => Ok(Some(false)),
            _ => Ok(None),
        }
    }

    fn eval_compare(&self, op: IRBinaryOp, l: &IRExpr, r: &IRExpr) -> EvalResult<bool> {
        let l = self.eval_expr(l)?;
        let r = self.eval_expr(r)?;

        match (op, &l, &r) {
            (_, None, _) | (_, _, None) => Ok(None),

            (IRBinaryOp::Eq, Some(a), Some(b)) => Ok(Some(a == b)),
            (IRBinaryOp::Gt, Some(Value::Int64(a)), Some(Value::Int64(b))) => Ok(Some(a > b)),

            _ => Err(ExecError::TypeMismatch {
                op: format!("{:?}", op),
                left: l.unwrap(),
                right: Some(r.unwrap()),
            }),
        }
    }

    fn eval_arithmetic(&self, op: IRBinaryOp, l: &IRExpr, r: &IRExpr) -> EvalResult<Value> {
        let lv = self.eval_expr(l)?;
        let rv = self.eval_expr(r)?;

        match (op, &lv, &rv) {
            (_, None, _) | (_, _, None) => Ok(None),

            (IRBinaryOp::Add, Some(Value::Int64(a)), Some(Value::Int64(b))) => {
                Ok(Some(Value::Int64(a + b)))
            }

            (IRBinaryOp::Div, _, Some(Value::Int64(0))) => Err(ExecError::DivisionByZero),

            _ => Err(ExecError::TypeMismatch {
                op: format!("{:?}", op),
                left: lv.unwrap(),
                right: rv,
            }),
        }
    }
    fn eval_unary(&self, op: IRUnaryOp, v: Option<Value>) -> EvalResult<Value> {
        match (op, &v) {
            (_, None) => Ok(None),

            (IRUnaryOp::Neg, Some(Value::Int64(x))) => Ok(Some(Value::Int64(-x))),

            _ => Err(ExecError::InvalidUnary {
                op: format!("{:?}", op),
                value: v.unwrap(),
            }),
        }
    }
}
