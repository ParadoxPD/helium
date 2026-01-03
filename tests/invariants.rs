use helium::ir::expr::Expr;

#[test]
#[should_panic]
fn unbound_column_must_not_reach_execution() {
    helium::exec::expr_eval::eval_value(&helium::ir::expr::Expr::col("age"), &Default::default());
}
