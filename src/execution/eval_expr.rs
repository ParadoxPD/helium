use crate::ir::expr::{BinaryOp, Expr, UnaryOp};
use crate::types::value::Value;

pub fn eval_expr(expr: &Expr, row: &[Value]) -> Value {
    match expr {
        Expr::Literal(v) => v.clone(),
        Expr::Null => Value::Null,

        Expr::BoundColumn { index, .. } => row[*index].clone(),

        Expr::Unary { op, expr } => {
            let v = eval_expr(expr, row);
            eval_unary(*op, v)
        }

        Expr::Binary { left, op, right } => {
            let l = eval_expr(left, row);
            let r = eval_expr(right, row);
            eval_binary(*op, l, r)
        }

        Expr::Column(_) => {
            panic!("Unbound column reached execution");
        }
    }
}

fn eval_unary(op: UnaryOp, v: Value) -> Value {
    match (op, v) {
        (_, Value::Null) => Value::Null,

        (UnaryOp::Neg, Value::Int64(x)) => Value::Int64(-x),
        (UnaryOp::Not, Value::Bool(b)) => Value::Bool(!b),

        _ => panic!("Invalid unary operation"),
    }
}

fn eval_binary(op: BinaryOp, l: Value, r: Value) -> Value {
    use BinaryOp::*;

    match (op, l, r) {
        (_, Value::Null, _) | (_, _, Value::Null) => Value::Null,

        (Add, Value::Int64(a), Value::Int64(b)) => Value::Int64(a + b),
        (Sub, Value::Int64(a), Value::Int64(b)) => Value::Int64(a - b),
        (Mul, Value::Int64(a), Value::Int64(b)) => Value::Int64(a * b),

        (Div, _, Value::Int64(0)) => panic!("Division by zero"),
        (Div, Value::Int64(a), Value::Int64(b)) => Value::Int64(a / b),

        (Eq, a, b) => Value::Bool(a == b),
        (Neq, a, b) => Value::Bool(a != b),

        (Lt, Value::Int64(a), Value::Int64(b)) => Value::Bool(a < b),
        (Lte, Value::Int64(a), Value::Int64(b)) => Value::Bool(a <= b),
        (Gt, Value::Int64(a), Value::Int64(b)) => Value::Bool(a > b),
        (Gte, Value::Int64(a), Value::Int64(b)) => Value::Bool(a >= b),

        (And, Value::Bool(a), Value::Bool(b)) => Value::Bool(a && b),
        (Or, Value::Bool(a), Value::Bool(b)) => Value::Bool(a || b),

        _ => panic!("Invalid binary operation"),
    }
}
