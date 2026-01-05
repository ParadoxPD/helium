#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::db::QueryError;
    use crate::buffer::buffer_pool::BufferPoolHandle;
    use crate::common::schema::{Column, Schema};
    use crate::common::types::DataType;
    use crate::exec::catalog::Catalog;
    use crate::frontend::sql::ast::{FromItem, Statement};
    use crate::frontend::sql::binder::{BindError, Binder, BoundStatement};
    use crate::frontend::sql::lower::{Lowered, lower_select};
    use crate::frontend::sql::parser::Parser;
    use crate::ir::pretty::pretty;
    use crate::storage::table::HeapTable;
    use crate::{buffer::buffer_pool::BufferPool, storage::page_manager::FilePageManager};
    use std::error::Error;
    use std::sync::{Arc, Mutex};

    fn test_catalog() -> Catalog {
        let mut catalog = Catalog::new();

        // Fake schema-only tables for binder tests
        catalog
            .create_table(
                "users".into(),
                Schema {
                    columns: vec![
                        Column {
                            name: "id".into(),
                            ty: DataType::Int64,
                            nullable: true,
                        },
                        Column {
                            name: "age".into(),
                            ty: DataType::Int64,
                            nullable: true,
                        },
                        Column {
                            name: "name".into(),
                            ty: DataType::String,
                            nullable: true,
                        },
                    ],
                },
                dummy_buffer_pool(),
            )
            .unwrap();

        catalog
            .create_table(
                "orders".into(),
                Schema {
                    columns: vec![Column {
                        name: "user_id".into(),
                        ty: DataType::Int64,
                        nullable: true,
                    }],
                },
                dummy_buffer_pool(),
            )
            .unwrap();

        catalog
    }

    fn bind(sql: &str) -> Result<BoundStatement, QueryError> {
        let stmt = Parser::new(sql).parse_statement()?;
        let catalog = test_catalog();
        let mut binder = Binder::new(&catalog);
        let bound = binder.bind_statement(stmt)?;
        Ok(bound)
    }

    fn dummy_buffer_pool() -> BufferPoolHandle {
        let path = format!("/tmp/helium_test_{}.db", rand::random::<u64>());
        Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path.into()).unwrap(),
        ))))
    }

    #[test]
    fn binds_simple_column() {
        let bound = bind("SELECT id FROM users;").unwrap();

        match bound {
            BoundStatement::Select(select) => {
                assert!(select.where_clause.is_none());
                assert_eq!(select.columns.len(), 1);
            }
            _ => panic!("expected BoundSelect"),
        }
    }

    #[test]
    fn errors_on_ambiguous_column() {
        let res = bind("SELECT id FROM users u JOIN users o ON u.id = o.id;");
        println!("{:?}", res);

        assert!(matches!(
            res,
            Err(QueryError::Bind(BindError::AmbiguousColumn(_)))
        ));
    }

    #[test]
    fn binds_qualified_column() {
        let bound = bind("SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id;").unwrap();

        match bound {
            BoundStatement::Select(select) => {
                assert_eq!(select.columns.len(), 1);
            }
            _ => panic!("expected BoundSelect"),
        }
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

    #[test]
    fn parses_and_predicates() {
        let sql = "SELECT name FROM users WHERE age > 18 AND score > 50;";
        let stmt = Parser::new(sql).parse_statement().unwrap();
        match stmt {
            Statement::Select(select) => {
                assert!(select.where_clause.is_some());
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parses_order_by() {
        let sql = "SELECT name FROM users ORDER BY age DESC, name ASC;";
        let stmt = Parser::new(sql).parse_statement().unwrap();
        match stmt {
            Statement::Select(s) => {
                println!("{:?}", s);
                assert_eq!(s.order_by.len(), 2);
                assert!(!s.order_by[0].asc);
                assert!(s.order_by[1].asc);
            }
            _ => panic!("expected select"),
        }
    }
    #[test]
    fn parses_simple_join() {
        let sql = "SELECT name FROM users u JOIN orders o ON u.id = o.user_id;";
        let stmt = Parser::new(sql).parse_statement().unwrap();

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
        let sql = "SELECT name FROM users u \
             JOIN orders o ON u.id = o.user_id \
             JOIN payments p ON o.id = p.order_id;";

        let stmt = Parser::new(sql).parse_statement().unwrap();
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
        let stmt = Parser::new(sql).parse_statement().unwrap();

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
        let sql = "DROP TABLE users";
        let stmt = Parser::new(sql).parse_statement().unwrap();
        matches!(stmt, Statement::DropTable(_));
    }
}
