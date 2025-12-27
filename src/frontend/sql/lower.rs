use crate::common::value::Value;
use crate::ir::expr::{BinaryOp as IRBinOp, Expr as IRExpr};
use crate::ir::plan::{LogicalPlan, Sort};

use super::ast::*;

pub enum Lowered {
    Plan(LogicalPlan),
    Explain { analyze: bool, plan: LogicalPlan },
}

pub fn lower_stmt(stmt: Statement) -> Lowered {
    match stmt {
        Statement::Select(s) => Lowered::Plan(lower_select(s)),
        Statement::Explain { analyze, stmt } => {
            let inner = lower_stmt(*stmt);
            match inner {
                Lowered::Plan(plan) => Lowered::Explain { analyze, plan },
                _ => unreachable!(),
            }
        }
    }
}

pub fn lower_select(stmt: SelectStmt) -> LogicalPlan {
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

    if !stmt.order_by.is_empty() {
        let keys = stmt
            .order_by
            .into_iter()
            .map(|o| (IRExpr::col(o.column), o.asc))
            .collect();

        plan = LogicalPlan::Sort(Sort {
            input: Box::new(plan),
            keys,
        });
    }

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
        let lowered = lower_stmt(stmt);

        let expected = r#"
Project [name]
└─ Scan users
"#;

        match lowered {
            Lowered::Plan(plan) => {
                assert_eq!(pretty(&plan).trim(), expected.trim());
            }
            _ => panic!("expected plan"),
        }
    }

    #[test]
    fn lowers_select_where_limit() {
        let sql = "SELECT name FROM users WHERE age > 18 LIMIT 5;";
        let stmt = parse(sql);
        let lowered = lower_stmt(stmt);

        let expected = r#"
Limit 5
└─ Project [name]
   └─ Filter (age Gt 18)
      └─ Scan users
"#;

        match lowered {
            Lowered::Plan(plan) => assert_eq!(pretty(&plan).trim(), expected.trim()),
            _ => unreachable!(),
        }
    }

    #[test]
    fn explain_select() {
        let stmt = parse("EXPLAIN SELECT name FROM users;");
        let lowered = lower_stmt(stmt);

        match lowered {
            Lowered::Explain { analyze, plan } => {
                assert!(!analyze);
                assert!(pretty(&plan).contains("Scan users"));
            }
            _ => panic!("expected explain"),
        }
    }

    #[test]
    fn lowers_order_by() {
        let stmt = parse("SELECT name FROM users ORDER BY age DESC;");
        let lowered = lower_stmt(stmt);

        match lowered {
            Lowered::Plan(plan) => {
                let p = pretty(&plan);
                assert!(p.contains("Sort"));
            }
            _ => panic!("expected plan"),
        }
    }
}
