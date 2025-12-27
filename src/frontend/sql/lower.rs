use crate::common::value::Value;
use crate::ir::expr::{BinaryOp as IRBinOp, Expr as IRExpr};
use crate::ir::plan::LogicalPlan;

use super::ast::*;

pub fn lower(stmt: SelectStmt) -> LogicalPlan {
    let mut plan = LogicalPlan::scan(&stmt.table);

    if let Some(expr) = stmt.where_clause {
        plan = plan.filter(lower_expr(expr));
    }

    let projections = stmt
        .columns
        .into_iter()
        .map(|c| (IRExpr::col(&c), c))
        .collect();

    plan = plan.project(projections);

    if let Some(limit) = stmt.limit {
        plan = plan.limit(limit);
    }

    plan
}

fn lower_expr(expr: Expr) -> IRExpr {
    match expr {
        Expr::Column(c) => IRExpr::col(c),

        Expr::LiteralInt(i) => IRExpr::lit(Value::Int64(i)),

        Expr::Binary { left, op, right } => {
            let ir_op = match op {
                BinaryOp::Eq => IRBinOp::Eq,
                BinaryOp::Gt => IRBinOp::Gt,
                BinaryOp::Lt => IRBinOp::Lt,
                BinaryOp::And => IRBinOp::And,
            };

            IRExpr::bin(lower_expr(*left), ir_op, lower_expr(*right))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frontend::sql::parser::parse;
    use crate::ir::pretty::pretty;

    #[test]
    fn lowers_simple_select() {
        let sql = "SELECT name FROM users;";
        let stmt = parse(sql);
        let plan = lower(stmt);

        let expected = r#"
Project [name]
└─ Scan users
"#;

        assert_eq!(pretty(&plan).trim(), expected.trim());
    }

    #[test]
    fn lowers_select_where_limit() {
        let sql = "SELECT name FROM users WHERE age > 18 LIMIT 5;";
        let stmt = parse(sql);
        let plan = lower(stmt);

        let expected = r#"
Limit 5
└─ Project [name]
   └─ Filter (age Gt 18)
      └─ Scan users
"#;

        assert_eq!(pretty(&plan).trim(), expected.trim());
    }
}
