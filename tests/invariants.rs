use helium::ir::expr::Expr;

#[test]
#[should_panic(expected = "reached execution")]
fn unbound_column_must_not_reach_execution() {
    use helium::exec::evaluator::Evaluator;
    use helium::ir::expr::Expr;

    let row = Default::default();
    let ev = Evaluator::new(&row);

    // This simulates a BUG: AST column leaking into execution
    ev.eval_expr(&Expr::col("age"));
}
