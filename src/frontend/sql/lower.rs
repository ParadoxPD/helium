use crate::frontend::sql::binder::{Binder, BoundFromItem, BoundSelect};
use crate::ir::expr::Expr as IRExpr;
use crate::ir::plan::{Join, LogicalPlan, Sort};

use super::ast::*;

pub enum Lowered {
    Plan(LogicalPlan),
    Explain { analyze: bool, plan: LogicalPlan },
}

pub fn lower_stmt(stmt: Statement) -> Lowered {
    match stmt {
        Statement::Select(s) => {
            let bound = Binder::bind(s).expect("bind error");
            Lowered::Plan(lower_select(bound))
        }
        Statement::Explain { analyze, stmt } => {
            let inner = lower_stmt(*stmt);
            match inner {
                Lowered::Plan(plan) => Lowered::Explain { analyze, plan },
                _ => unreachable!(),
            }
        }
    }
}

pub fn lower_select(stmt: BoundSelect) -> LogicalPlan {
    let mut plan = lower_from(stmt.from);

    if let Some(expr) = stmt.where_clause {
        plan = plan.filter(expr);
    }

    if !stmt.order_by.is_empty() {
        let keys = stmt.order_by;

        plan = LogicalPlan::Sort(Sort {
            input: Box::new(plan),
            keys,
        });
    }

    let projections: Vec<(IRExpr, String)> = stmt
        .columns
        .into_iter()
        .map(|expr| {
            let name = output_name(&expr);
            (expr, name)
        })
        .collect();

    plan = plan.project(projections);

    if let Some(limit) = stmt.limit {
        plan = plan.limit(limit);
    }

    plan
}

fn lower_from(from: BoundFromItem) -> LogicalPlan {
    match from {
        BoundFromItem::Table { name, alias } => LogicalPlan::scan(&name, &alias),

        BoundFromItem::Join { left, right, on } => LogicalPlan::Join(Join {
            left: Box::new(lower_from(*left)),
            right: Box::new(lower_from(*right)),
            on,
        }),
    }
}

fn output_name(expr: &IRExpr) -> String {
    match expr {
        IRExpr::BoundColumn { name, .. } => name.clone(),
        _ => "expr".to_string(),
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

#[cfg(test)]
mod join_lowering_tests {
    use super::*;
    use crate::frontend::sql::parser::parse;
    use crate::ir::pretty::pretty;

    #[test]
    fn lowers_join_into_logical_plan() {
        let stmt = parse("SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id;");

        let lowered = lower_stmt(stmt);

        match lowered {
            Lowered::Plan(plan) => {
                let p = pretty(&plan);
                assert!(p.contains("Join"));
                assert!(p.contains("Scan users"));
                assert!(p.contains("Scan orders"));
            }
            _ => panic!("expected plan"),
        }
    }
}
