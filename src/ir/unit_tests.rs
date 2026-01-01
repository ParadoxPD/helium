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
        let expr = Expr::bound_col("t", "age");
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
            Expr::bound_col("t", "salary"),
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
        let expr = Expr::bin(
            Expr::bound_col("t", "age"),
            BinaryOp::Gt,
            Expr::lit(Value::Int64(18)),
        );

        let printed = format!("{expr}");
        assert!(printed.contains("age"));
        assert!(printed.contains("Gt"));
        assert!(printed.contains("18"));
    }

    #[test]
    fn nested_expression_structure() {
        let expr = Expr::bin(
            Expr::bin(
                Expr::bound_col("t", "a"),
                BinaryOp::Add,
                Expr::lit(Value::Int64(1)),
            ),
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
    use super::*;
    use crate::common::value::Value;
    use crate::ir::expr::{BinaryOp, Expr};

    #[test]
    fn scan_has_no_children() {
        let plan = LogicalPlan::scan("users", "u");

        assert_eq!(plan.arity(), 0);
        assert!(plan.input().is_none());
    }

    #[test]
    fn filter_wraps_input() {
        let plan = LogicalPlan::scan("users", "u").filter(Expr::bin(
            Expr::bound_col("t", "age"),
            BinaryOp::Gt,
            Expr::lit(Value::Int64(18)),
        ));

        assert_eq!(plan.arity(), 1);
        assert!(plan.input().is_some());
    }

    #[test]
    fn project_preserves_structure() {
        let plan = LogicalPlan::scan("users", "u").project(vec![
            (Expr::bound_col("t", "name"), "name"),
            (Expr::bound_col("t", "city"), "city"),
        ]);

        match plan {
            LogicalPlan::Project(p) => {
                assert_eq!(p.exprs.len(), 2);
                assert_eq!(p.input.arity(), 0);
            }
            _ => panic!("expected Project node"),
        }
    }

    #[test]
    fn limit_wraps_project() {
        let plan = LogicalPlan::scan("users", "u")
            .project(vec![(Expr::bound_col("t", "name"), "name")])
            .limit(10);

        match plan {
            LogicalPlan::Limit(l) => {
                assert_eq!(l.count, 10);

                match *l.input {
                    LogicalPlan::Project(_) => {}
                    _ => panic!("expected Project under Limit"),
                }
            }
            _ => panic!("expected Limit node"),
        }
    }

    #[test]
    fn chaining_builds_correct_tree_shape() {
        let plan = LogicalPlan::scan("users", "u")
            .filter(Expr::bin(
                Expr::bound_col("t", "active"),
                BinaryOp::Eq,
                Expr::lit(Value::Bool(true)),
            ))
            .project(vec![(Expr::bound_col("t", "email"), "email")])
            .limit(5);

        // Limit
        let limit = match plan {
            LogicalPlan::Limit(l) => l,
            _ => panic!("expected Limit"),
        };

        // Project
        let project = match *limit.input {
            LogicalPlan::Project(p) => p,
            _ => panic!("expected Project"),
        };

        // Filter
        let filter = match *project.input {
            LogicalPlan::Filter(f) => f,
            _ => panic!("expected Filter"),
        };

        // Scan
        match *filter.input {
            LogicalPlan::Scan(_) => {}
            _ => panic!("expected Scan"),
        }
    }
    use super::*;
    use crate::common::value::Value;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::ir::plan::LogicalPlan;

    #[test]
    fn pretty_print_scan() {
        let plan = LogicalPlan::scan("users", "u");

        let output = pretty(&plan);

        assert_eq!(output.trim(), "Scan users");
    }

    #[test]
    fn pretty_print_filter() {
        let plan = LogicalPlan::scan("users", "u").filter(Expr::bin(
            Expr::bound_col("t", "age"),
            BinaryOp::Gt,
            Expr::lit(Value::Int64(18)),
        ));

        let output = pretty(&plan);

        assert!(output.contains("Filter"));
        assert!(output.contains("Scan users"));
    }

    #[test]
    fn pretty_print_project_chain() {
        let plan = LogicalPlan::scan("users", "u")
            .filter(Expr::bin(
                Expr::bound_col("t", "active"),
                BinaryOp::Eq,
                Expr::lit(Value::Bool(true)),
            ))
            .project(vec![
                (Expr::bound_col("t", "email"), "email"),
                (Expr::bound_col("t", "name"), "name"),
            ])
            .limit(5);

        let output = pretty(&plan);

        let expected = r#"
Limit 5
└─ Project [email, name]
   └─ Filter (active Eq true)
      └─ Scan users
"#;

        assert_eq!(output.trim(), expected.trim());
    }

    #[test]
    fn pretty_output_is_stable() {
        let plan = LogicalPlan::scan("users", "u").limit(1);

        let a = pretty(&plan);
        let b = pretty(&plan);

        assert_eq!(a, b);
    }
    use super::*;
    use crate::common::value::Value;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::ir::plan::LogicalPlan;

    #[test]
    fn valid_simple_scan() {
        let plan = LogicalPlan::scan("users", "u");
        assert_eq!(validate(&plan), Ok(()));
    }

    #[test]
    fn valid_filter_plan() {
        let plan = LogicalPlan::scan("users", "u").filter(Expr::bin(
            Expr::bound_col("t", "age"),
            BinaryOp::Gt,
            Expr::lit(Value::Int64(18)),
        ));

        assert_eq!(validate(&plan), Ok(()));
    }

    #[test]
    fn project_must_not_be_empty() {
        let plan = LogicalPlan::Project(crate::ir::plan::Project {
            input: Box::new(LogicalPlan::scan("users", "u")),
            exprs: vec![],
        });

        assert_eq!(validate(&plan), Err(ValidationError::EmptyProject));
    }

    #[test]
    fn limit_must_be_positive() {
        let plan = LogicalPlan::scan("users", "u").limit(0);

        assert_eq!(validate(&plan), Err(ValidationError::ZeroLimit));
    }

    #[test]
    fn filter_predicate_cannot_be_null() {
        let plan = LogicalPlan::scan("users", "u").filter(Expr::Null);

        assert_eq!(validate(&plan), Err(ValidationError::NullPredicate));
    }

    #[test]
    fn deeply_nested_plan_validates() {
        let plan = LogicalPlan::scan("users", "u")
            .filter(Expr::bin(
                Expr::bound_col("t", "active"),
                BinaryOp::Eq,
                Expr::lit(Value::Bool(true)),
            ))
            .project(vec![(Expr::bound_col("t", "email"), "email")])
            .limit(10);

        assert_eq!(validate(&plan), Ok(()));
    }
}
