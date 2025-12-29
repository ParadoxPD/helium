use crate::common::value::Value;
use crate::ir::expr::{BinaryOp, Expr, UnaryOp};
use crate::ir::plan::{Filter, Join, LogicalPlan, Project};

pub fn constant_fold(plan: &LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan(_) => plan.clone(),
        LogicalPlan::Filter(filter) => LogicalPlan::Filter(Filter {
            input: Box::new(constant_fold(&filter.input)),
            predicate: fold_expr(&filter.predicate),
        }),
        LogicalPlan::Project(project) => LogicalPlan::Project(Project {
            input: Box::new(constant_fold(&project.input)),
            exprs: project
                .exprs
                .iter()
                .map(|(e, a)| (fold_expr(e), a.clone()))
                .collect(),
        }),
        LogicalPlan::Sort(sort) => LogicalPlan::Sort(crate::ir::plan::Sort {
            input: Box::new(constant_fold(&sort.input)),
            keys: sort
                .keys
                .iter()
                .map(|(e, asc)| (fold_expr(e), *asc))
                .collect(),
        }),
        LogicalPlan::Limit(limit) => LogicalPlan::Limit(limit.clone()),
        LogicalPlan::Join(join) => LogicalPlan::Join(Join {
            left: Box::new(constant_fold(&join.left)),
            right: Box::new(constant_fold(&join.right)),
            on: fold_expr(&join.on),
        }),
    }
}

pub fn fold_expr(expr: &Expr) -> Expr {
    match expr {
        Expr::Literal(_) | Expr::Column(_) | Expr::Null | Expr::BoundColumn { .. } => expr.clone(),
        Expr::Unary { op, expr } => {
            let folded = fold_expr(expr);

            match (op, &folded) {
                (UnaryOp::Neg, Expr::Literal(Value::Int64(v))) => Expr::lit(Value::Int64(-v)),
                (UnaryOp::Not, Expr::Literal(Value::Bool(v))) => Expr::lit(Value::Bool(!v)),
                _ => Expr::unary(*op, folded),
            }
        }
        Expr::Binary { left, op, right } => {
            let l = fold_expr(left);
            let r = fold_expr(right);

            match (&l, op, &r) {
                (Expr::Literal(Value::Int64(a)), BinaryOp::Add, Expr::Literal(Value::Int64(b))) => {
                    Expr::lit(Value::Int64(a + b))
                }

                (Expr::Literal(Value::Int64(a)), BinaryOp::Sub, Expr::Literal(Value::Int64(b))) => {
                    Expr::lit(Value::Int64(a - b))
                }
                (Expr::Literal(Value::Int64(a)), BinaryOp::Mul, Expr::Literal(Value::Int64(b))) => {
                    Expr::lit(Value::Int64(a * b))
                }
                (Expr::Literal(Value::Int64(a)), BinaryOp::Eq, Expr::Literal(Value::Int64(b))) => {
                    Expr::lit(Value::Bool(a == b))
                }
                (Expr::Literal(Value::Int64(a)), BinaryOp::Gt, Expr::Literal(Value::Int64(b))) => {
                    Expr::lit(Value::Bool(a > b))
                }
                (Expr::Literal(Value::Bool(a)), BinaryOp::And, Expr::Literal(Value::Bool(b))) => {
                    Expr::lit(Value::Bool(*a && *b))
                }
                (Expr::Literal(Value::Bool(a)), BinaryOp::Or, Expr::Literal(Value::Bool(b))) => {
                    Expr::lit(Value::Bool(*a || *b))
                }
                _ => Expr::bin(l, *op, r),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::value::Value;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::ir::plan::LogicalPlan;
    use crate::ir::pretty::pretty;

    #[test]
    fn folds_simple_arithmetic() {
        let expr = Expr::bin(
            Expr::lit(Value::Int64(10)),
            BinaryOp::Add,
            Expr::lit(Value::Int64(20)),
        );

        let folded = fold_expr(&expr);
        assert_eq!(folded, Expr::lit(Value::Int64(30)));
    }

    #[test]
    fn folds_nested_arithmetic() {
        let expr = Expr::bin(
            Expr::bin(
                Expr::lit(Value::Int64(2)),
                BinaryOp::Mul,
                Expr::lit(Value::Int64(5)),
            ),
            BinaryOp::Add,
            Expr::lit(Value::Int64(1)),
        );

        let folded = fold_expr(&expr);
        assert_eq!(folded, Expr::lit(Value::Int64(11)));
    }

    #[test]
    fn does_not_fold_column_expressions() {
        let expr = Expr::bin(
            Expr::bound_col("t", "age"),
            BinaryOp::Add,
            Expr::lit(Value::Int64(1)),
        );

        let folded = fold_expr(&expr);
        assert_eq!(folded, expr);
    }

    #[test]
    fn folds_boolean_logic() {
        let expr = Expr::bin(
            Expr::lit(Value::Bool(true)),
            BinaryOp::And,
            Expr::lit(Value::Bool(false)),
        );

        let folded = fold_expr(&expr);
        assert_eq!(folded, Expr::lit(Value::Bool(false)));
    }

    #[test]
    fn folds_filter_predicate_in_plan() {
        let plan = LogicalPlan::scan("users", "u").filter(Expr::bin(
            Expr::bin(
                Expr::lit(Value::Int64(10)),
                BinaryOp::Gt,
                Expr::lit(Value::Int64(5)),
            ),
            BinaryOp::And,
            Expr::lit(Value::Bool(true)),
        ));

        let optimized = constant_fold(&plan);

        let expected = r#"
Filter (true)
└─ Scan users
"#;

        assert_eq!(pretty(&optimized).trim(), expected.trim());
    }

    #[test]
    fn optimizer_is_idempotent() {
        let plan = LogicalPlan::scan("users", "u").filter(Expr::bin(
            Expr::lit(Value::Int64(1)),
            BinaryOp::Eq,
            Expr::lit(Value::Int64(1)),
        ));

        let once = constant_fold(&plan);
        let twice = constant_fold(&once);

        assert_eq!(pretty(&once), pretty(&twice));
    }
}
