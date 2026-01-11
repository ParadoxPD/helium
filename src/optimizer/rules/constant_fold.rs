use crate::{
    ir::{
        expr::{BinaryOp, Expr, UnaryOp},
        plan::LogicalPlan,
    },
    optimizer::errors::OptimizerError,
    types::value::Value,
};

pub fn constant_fold(plan: &LogicalPlan) -> Result<LogicalPlan, OptimizerError> {
    Ok(match plan {
        LogicalPlan::Scan { .. } | LogicalPlan::IndexScan { .. } => plan.clone(),

        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(constant_fold(input)?),
            predicate: fold_expr(predicate),
        },

        LogicalPlan::Project { input, exprs } => LogicalPlan::Project {
            input: Box::new(constant_fold(input)?),
            exprs: exprs.iter().map(fold_expr).collect(),
        },

        LogicalPlan::Sort { input, keys } => LogicalPlan::Sort {
            input: Box::new(constant_fold(input)?),
            keys: keys.clone(),
        },

        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => LogicalPlan::Limit {
            input: Box::new(constant_fold(input)?),
            limit: *limit,
            offset: *offset,
        },

        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
        } => LogicalPlan::Join {
            left: Box::new(constant_fold(left)?),
            right: Box::new(constant_fold(right)?),
            on: fold_expr(on),
            join_type: *join_type,
        },

        _ => plan.clone(),
    })
}

pub fn fold_expr(expr: &Expr) -> Expr {
    match expr {
        Expr::Literal(_) | Expr::Null | Expr::BoundColumn { .. } => expr.clone(),

        Expr::Unary { op, expr } => {
            let e = fold_expr(expr);
            match (op, &e) {
                (UnaryOp::Neg, Expr::Literal(Value::Int64(v))) => Expr::Literal(Value::Int64(-v)),
                (UnaryOp::Not, Expr::Literal(Value::Boolean(v))) => {
                    Expr::Literal(Value::Boolean(!v))
                }
                _ => Expr::Unary {
                    op: *op,
                    expr: Box::new(e),
                },
            }
        }

        Expr::Binary { left, op, right } => {
            let l = fold_expr(left);
            let r = fold_expr(right);

            match (&l, op, &r) {
                (Expr::Literal(Value::Int64(a)), BinaryOp::Add, Expr::Literal(Value::Int64(b))) => {
                    Expr::Literal(Value::Int64(a + b))
                }

                (Expr::Literal(Value::Int64(a)), BinaryOp::Eq, Expr::Literal(Value::Int64(b))) => {
                    Expr::Literal(Value::Boolean(a == b))
                }

                (
                    Expr::Literal(Value::Boolean(a)),
                    BinaryOp::And,
                    Expr::Literal(Value::Boolean(b)),
                ) => Expr::Literal(Value::Boolean(*a && *b)),

                _ => Expr::Binary {
                    left: Box::new(l),
                    op: *op,
                    right: Box::new(r),
                },
            }
        }
    }
}

