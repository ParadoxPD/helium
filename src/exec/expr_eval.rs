use crate::common::value::Value;
use crate::exec::operator::Row;
use crate::ir::expr::{BinaryOp, Expr, UnaryOp};

pub fn eval_value(expr: &Expr, row: &Row) -> Value {
    match expr {
        Expr::Column(_) => {
            panic!("BUG: Expr::Column reached execution — must be BoundColumn");
        }
        Expr::Literal(v) => v.clone(),

        Expr::Unary { op, expr } => {
            let v = eval_value(expr, row);
            match (op, v) {
                (UnaryOp::Neg, Value::Int64(x)) => Value::Int64(-x),
                (UnaryOp::Not, Value::Bool(b)) => Value::Bool(!b),
                _ => Value::Null,
            }
        }

        Expr::Binary { left, op, right } => {
            let l = eval_value(left, row);
            let r = eval_value(right, row);

            match (l, op, r) {
                (Value::Int64(a), BinaryOp::Add, Value::Int64(b)) => Value::Int64(a + b),
                (Value::Int64(a), BinaryOp::Sub, Value::Int64(b)) => Value::Int64(a - b),
                (Value::Int64(a), BinaryOp::Mul, Value::Int64(b)) => Value::Int64(a * b),
                _ => Value::Null,
            }
        }

        Expr::BoundColumn { table, name } => row
            .get(&format!("{table}.{name}"))
            .cloned()
            .unwrap_or(Value::Null),

        Expr::Null => Value::Null,
    }
}

pub fn eval_predicate(expr: &Expr, row: &Row) -> bool {
    match expr {
        Expr::Binary { left, op, right } => match op {
            BinaryOp::And => eval_predicate(left, row) && eval_predicate(right, row),
            BinaryOp::Or => eval_predicate(left, row) || eval_predicate(right, row),

            BinaryOp::Eq => eval_value(left, row) == eval_value(right, row),

            BinaryOp::Gt => match (eval_value(left, row), eval_value(right, row)) {
                (Value::Int64(a), Value::Int64(b)) => a > b,
                _ => false,
            },

            BinaryOp::Lt => match (eval_value(left, row), eval_value(right, row)) {
                (Value::Int64(a), Value::Int64(b)) => a < b,
                _ => false,
            },

            _ => false,
        },

        Expr::Literal(Value::Bool(b)) => *b,
        _ => false,
    }
}
