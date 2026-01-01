#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::catalog::Catalog;
    use crate::frontend::sql::parser::parse;
    use crate::storage::table::HeapTable;
    use crate::{buffer::buffer_pool::BufferPool, storage::page_manager::FilePageManager};
    use std::sync::{Arc, Mutex};

    fn test_catalog() -> Catalog {
        let mut catalog = Catalog::new();

        let path = format!("/tmp/db_{}.db", rand::random::<u64>());

        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        // -------- create table --------
        let user_schema = vec!["id".into(), "name".into()];
        let mut users_table = HeapTable::new("users".into(), user_schema.clone(), 4, bp.clone());

        catalog.insert("users".into(), Arc::new(users_table));

        let order_schema = vec!["user_id".into()];
        let mut orders_table = HeapTable::new("users".into(), order_schema.clone(), 4, bp.clone());
        catalog.insert("orders".into(), Arc::new(orders_table));

        catalog
    }

    #[test]
    fn binds_simple_column() {
        let stmt = parse("SELECT id FROM users;");
        let catalog = test_catalog();

        let bound = match stmt {
            crate::frontend::sql::ast::Statement::Select(s) => Binder::bind(s, &catalog).unwrap(),
            _ => panic!(),
        };

        assert!(bound.where_clause.is_none());
    }

    #[test]
    fn errors_on_ambiguous_column() {
        let stmt = parse("SELECT id FROM users u JOIN orders o ON u.id = o.user_id;");
        let catalog = test_catalog();

        let res = match stmt {
            crate::frontend::sql::ast::Statement::Select(s) => Binder::bind(s, &catalog),
            _ => panic!(),
        };

        assert!(matches!(res, Err(BindError::AmbiguousColumn(_))));
    }

    #[test]
    fn binds_qualified_column() {
        let stmt = parse("SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id;");
        let catalog = test_catalog();

        let bound = match stmt {
            crate::frontend::sql::ast::Statement::Select(s) => Binder::bind(s, &catalog).unwrap(),
            _ => panic!(),
        };

        assert_eq!(bound.columns.len(), 1);
    }

    #[test]
    fn reject_duplicate_columns() {
        let sql = "CREATE TABLE t (id INT, id INT)";
        assert!(bind(sql).is_err());
    }

    #[test]
    fn reject_empty_table() {
        let sql = "CREATE TABLE t ()";
        assert!(bind(sql).is_err());
    }
    use super::*;

    #[test]
    fn parses_and_predicates() {
        let sql = "SELECT name FROM users WHERE age > 18 AND score > 50;";
        let stmt = parse(sql);
        match stmt {
            Statement::Select(select) => {
                assert!(select.where_clause.is_some());
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parses_order_by() {
        let stmt = parse("SELECT name FROM users ORDER BY age DESC, name ASC;");
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.order_by.len(), 2);
                assert!(!s.order_by[0].asc);
                assert!(s.order_by[1].asc);
            }
            _ => panic!("expected select"),
        }
    }
}

#[cfg(test)]
mod join_tests {
    use super::*;
    use crate::frontend::sql::ast::{FromItem, Statement};

    #[test]
    fn parses_simple_join() {
        let stmt = parse("SELECT name FROM users u JOIN orders o ON u.id = o.user_id;");

        match stmt {
            Statement::Select(s) => match s.from {
                FromItem::Join { left, right, .. } => {
                    assert!(matches!(*left, FromItem::Table { .. }));
                    assert!(matches!(*right, FromItem::Table { .. }));
                }
                _ => panic!("expected join"),
            },
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parses_chained_joins() {
        let stmt = parse(
            "SELECT name FROM users u \
             JOIN orders o ON u.id = o.user_id \
             JOIN payments p ON o.id = p.order_id;",
        );

        match stmt {
            Statement::Select(s) => match s.from {
                FromItem::Join { left, .. } => {
                    // left itself should be another join
                    assert!(matches!(*left, FromItem::Join { .. }));
                }
                _ => panic!("expected join"),
            },
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parse_create_table() {
        let sql = "CREATE TABLE users (id INT, name TEXT)";
        let stmt = parse(sql);

        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.table_name, "users");
                assert_eq!(ct.columns.len(), 2);
            }
            _ => panic!("wrong statement"),
        }
    }

    #[test]
    fn parse_drop_table() {
        let stmt = parse("DROP TABLE users");
        matches!(stmt, Statement::DropTable(_));
    }
    use super::*;
    use crate::exec::catalog::Catalog;
    use crate::frontend::sql::parser::parse;
    use crate::ir::pretty::pretty;

    #[test]
    fn lowers_simple_select() {
        let sql = "SELECT name FROM users;";
        let stmt = parse(sql);
        let catalog = Catalog::new();

        let lowered = lower_stmt(stmt, &catalog);

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
        let catalog = Catalog::new();

        let lowered = lower_stmt(stmt, &catalog);

        let expected = r#"
Limit 5
└─ Project [name]
   └─ Filter (age Gt 18)
      └─ Scan users
"#;

        match lowered {
            Lowered::Plan(plan) => {
                assert_eq!(pretty(&plan).trim(), expected.trim())
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn explain_select() {
        let stmt = parse("EXPLAIN SELECT name FROM users;");
        let catalog = Catalog::new();

        let lowered = lower_stmt(stmt, &catalog);

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
        let catalog = Catalog::new();

        let lowered = lower_stmt(stmt, &catalog);

        match lowered {
            Lowered::Plan(plan) => {
                let p = pretty(&plan);
                assert!(p.contains("Sort"));
            }
            _ => panic!("expected plan"),
        }
    }

    #[test]
    fn lowers_join_into_logical_plan() {
        let stmt = parse("SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id;");
        let catalog = Catalog::new();

        let lowered = lower_stmt(stmt, &catalog);

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
