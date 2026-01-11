use crate::catalog::ids::ColumnId;
use crate::execution::errors::ExecutionError;
use crate::execution::executor::ExecResult;
use crate::ir::expr::{BinaryOp, Expr, UnaryOp};
use crate::types::value::Value;

pub fn eval_expr(expr: &Expr, row: &[Value]) -> ExecResult<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Null => Ok(Value::Null),

        Expr::BoundColumn { column_id } => {
            let ColumnId(idx) = column_id;
            row.get(*idx as usize)
                .cloned()
                .ok_or(ExecutionError::ColumnOutOfBounds {
                    index: *idx as usize,
                    column_count: row.len(),
                })
        }

        Expr::Unary { op, expr } => {
            let v = eval_expr(expr, row)?;
            eval_unary(*op, v)
        }

        Expr::Binary { left, op, right } => {
            let l = eval_expr(left, row)?;
            let r = eval_expr(right, row)?;
            eval_binary(*op, l, r)
        }
    }
}

fn eval_unary(op: UnaryOp, v: Value) -> ExecResult<Value> {
    match (op, v) {
        (_, Value::Null) => Ok(Value::Null),

        (UnaryOp::Neg, Value::Int64(x)) => Ok(Value::Int64(-x)),
        (UnaryOp::Not, Value::Boolean(b)) => Ok(Value::Boolean(!b)),

        _ => Err(ExecutionError::InvalidExpression {
            reason: "invalid unary operation".into(),
        }),
    }
}

fn eval_binary(op: BinaryOp, l: Value, r: Value) -> ExecResult<Value> {
    use BinaryOp::*;

    if matches!(l, Value::Null) || matches!(r, Value::Null) {
        return Ok(Value::Null);
    }

    match (op, l, r) {
        (Add, Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a + b)),
        (Sub, Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a - b)),
        (Mul, Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a * b)),

        (Div, _, Value::Int64(0)) => Err(ExecutionError::DivisionByZero),
        (Div, Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a / b)),

        (Eq, a, b) => Ok(Value::Boolean(a == b)),
        (Neq, a, b) => Ok(Value::Boolean(a != b)),

        (Lt, Value::Int64(a), Value::Int64(b)) => Ok(Value::Boolean(a < b)),
        (Lte, Value::Int64(a), Value::Int64(b)) => Ok(Value::Boolean(a <= b)),
        (Gt, Value::Int64(a), Value::Int64(b)) => Ok(Value::Boolean(a > b)),
        (Gte, Value::Int64(a), Value::Int64(b)) => Ok(Value::Boolean(a >= b)),

        (And, Value::Boolean(a), Value::Boolean(b)) => Ok(Value::Boolean(a && b)),
        (Or, Value::Boolean(a), Value::Boolean(b)) => Ok(Value::Boolean(a || b)),

        (op, l, r) => Err(ExecutionError::TypeMismatch {
            op: format!("{:?}", op),
            left: l,
            right: r,
        }),
    }
}

