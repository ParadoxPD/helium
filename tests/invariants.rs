use helium::ir::expr::Expr;

#[test]
#[should_panic]
fn unbound_column_must_not_reach_execution() {
    // This should panic once invariants are enforced
    helium::exec::expr_eval::eval_value(&Expr::col("age"), &Default::default());
}
