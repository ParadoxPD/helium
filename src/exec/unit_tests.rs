#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        api::db::{Database, QueryResult},
        common::value::Value,
        exec::operator::Row,
        frontend::sql::{binder::Binder, parser::parse},
    };

    pub fn test_db() -> Database {
        let path = format!("/tmp/helium_test_{}.db", rand::random::<u64>());
        Database::open(path).unwrap()
    }
    pub fn exec_sql(db: &mut Database, sql: &str) -> QueryResult {
        db.run_query(sql).unwrap()
    }
    pub fn insert_values(db: &mut Database, table: &str, rows: Vec<Vec<Value>>) {
        let entry = db.catalog.get_table(table).unwrap();
        entry.heap.insert_rows(rows);
    }

    #[test]
    fn table_cursor_emits_distinct_row_ids() {
        let mut db = test_db();

        exec_sql(&mut db, "CREATE TABLE t (id INT)");

        insert_values(
            &mut db,
            "t",
            vec![vec![Value::Int64(1)], vec![Value::Int64(2)]],
        );

        let table = db.catalog.get_table("t").unwrap();
        let mut cursor = table.heap.scan();

        let (r1, _) = cursor.next().unwrap();
        let (r2, _) = cursor.next().unwrap();

        assert_ne!(r1, r2);
    }
    #[test]
    fn execute_simple_scan() {
        let mut db = test_db();

        exec_sql(&mut db, "CREATE TABLE users (id INT)");

        insert_values(
            &mut db,
            "users",
            vec![vec![Value::Int64(1)], vec![Value::Int64(2)]],
        );

        match exec_sql(&mut db, "SELECT id FROM users") {
            QueryResult::Rows(rows) => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("expected rows"),
        }
    }
    #[test]
    fn filter_removes_rows() {
        let mut db = test_db();

        exec_sql(&mut db, "CREATE TABLE users (age INT)");

        insert_values(
            &mut db,
            "users",
            vec![vec![Value::Int64(10)], vec![Value::Int64(30)]],
        );

        match exec_sql(&mut db, "SELECT age FROM users WHERE age > 18") {
            QueryResult::Rows(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].values.get("age"), Some(&Value::Int64(30)));
            }
            _ => panic!("expected rows"),
        }
    }
    #[test]
    fn project_exec_works() {
        let mut db = test_db();

        exec_sql(&mut db, "CREATE TABLE users (id INT, age INT)");

        insert_values(
            &mut db,
            "users",
            vec![vec![Value::Int64(1), Value::Int64(30)]],
        );

        match exec_sql(&mut db, "SELECT age FROM users") {
            QueryResult::Rows(rows) => {
                assert!(rows[0].values.contains_key("age"));
                assert!(!rows[0].values.contains_key("id"));
            }
            _ => panic!("expected rows"),
        }
    }
    #[test]
    fn join_matches_rows() {
        let mut db = test_db();

        exec_sql(&mut db, "CREATE TABLE users (id INT, name INT)");
        exec_sql(&mut db, "CREATE TABLE orders (user_id INT, amount INT)");

        insert_values(
            &mut db,
            "users",
            vec![vec![Value::Int64(1), Value::Int64(10)]],
        );
        insert_values(
            &mut db,
            "orders",
            vec![vec![Value::Int64(1), Value::Int64(99)]],
        );

        match exec_sql(
            &mut db,
            "SELECT users.id FROM users JOIN orders ON users.id = orders.user_id",
        ) {
            QueryResult::Rows(rows) => assert_eq!(rows.len(), 1),
            _ => panic!("expected rows"),
        }
    }
    #[test]
    fn create_index_registers_in_catalog() {
        let mut db = test_db();

        exec_sql(&mut db, "CREATE TABLE users (id INT, age INT)");
        exec_sql(&mut db, "CREATE INDEX idx_users_age ON users(age)");

        assert!(db.catalog.get_index("users", "age").is_some());
    }
    #[test]
    fn drop_index_removes_from_catalog() {
        let mut db = test_db();

        exec_sql(&mut db, "CREATE TABLE users (id INT, age INT)");
        exec_sql(&mut db, "CREATE INDEX idx_users_age ON users(age)");

        exec_sql(&mut db, "DROP INDEX idx_users_age");

        assert!(db.catalog.get_index("users", "age").is_none());
    }

    #[test]
    fn execute_filter_project_limit() {
        let mut db = test_db();

        exec_sql(&mut db, "CREATE TABLE users (name TEXT, age INT)");

        insert_values(
            &mut db,
            "users",
            vec![
                vec![Value::String("Alice".into()), Value::Int64(30)],
                vec![Value::String("Bob".into()), Value::Int64(15)],
                vec![Value::String("Carol".into()), Value::Int64(40)],
            ],
        );

        let result = exec_sql(&mut db, "SELECT name FROM users WHERE age > 18 LIMIT 2");

        match result {
            QueryResult::Rows(rows) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(
                    rows[0].values.get("name"),
                    Some(&Value::String("Alice".into()))
                );
                assert_eq!(
                    rows[1].values.get("name"),
                    Some(&Value::String("Carol".into()))
                );
            }
            _ => panic!("expected rows"),
        }
    }
    #[test]
    fn execution_respects_optimizer_output() {
        let mut db = test_db();

        exec_sql(&mut db, "CREATE TABLE users (x INT)");

        insert_values(
            &mut db,
            "users",
            vec![
                vec![Value::Int64(1)],
                vec![Value::Int64(2)],
                vec![Value::Int64(3)],
            ],
        );

        let result = exec_sql(&mut db, "SELECT x FROM users WHERE x > 1 LIMIT 1");

        match result {
            QueryResult::Rows(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].values.get("x"), Some(&Value::Int64(2)));
            }
            _ => panic!("expected rows"),
        }
    }
    #[test]
    fn index_scan_end_to_end() {
        let mut db = test_db();

        exec_sql(&mut db, "CREATE TABLE t (id INT)");

        insert_values(
            &mut db,
            "t",
            (0..10).map(|i| vec![Value::Int64(i)]).collect(),
        );

        exec_sql(&mut db, "CREATE INDEX idx_t_id ON t(id)");

        let result = exec_sql(&mut db, "SELECT id FROM t WHERE id = 5");

        match result {
            QueryResult::Rows(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].values.get("id"), Some(&Value::Int64(5)));
            }
            _ => panic!("expected rows"),
        }
    }
    #[test]
    fn index_scan_exec_returns_matching_rows() {
        let mut db = test_db();

        exec_sql(&mut db, "CREATE TABLE users (id INT, age INT)");

        insert_values(
            &mut db,
            "users",
            vec![
                vec![Value::Int64(1), Value::Int64(20)],
                vec![Value::Int64(2), Value::Int64(20)],
                vec![Value::Int64(3), Value::Int64(30)],
                vec![Value::Int64(4), Value::Int64(40)],
            ],
        );

        exec_sql(&mut db, "CREATE INDEX idx_users_age ON users(age)");

        let result = exec_sql(&mut db, "SELECT * FROM users WHERE age = 20");

        match result {
            QueryResult::Rows(rows) => {
                assert_eq!(rows.len(), 2);
                for r in rows {
                    assert_eq!(r.values.get("age"), Some(&Value::Int64(20)));
                }
            }
            _ => panic!("expected rows"),
        }
    }
    #[test]
    fn limit_returns_only_n_rows() {
        let mut db = test_db();

        exec_sql(&mut db, "CREATE TABLE users (x INT)");
        insert_values(
            &mut db,
            "users",
            vec![
                vec![Value::Int64(1)],
                vec![Value::Int64(2)],
                vec![Value::Int64(3)],
            ],
        );

        let res = exec_sql(&mut db, "SELECT x FROM users LIMIT 2");

        match res {
            QueryResult::Rows(rows) => assert_eq!(rows.len(), 2),
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn limit_zero_returns_no_rows() {
        let mut db = test_db();

        exec_sql(&mut db, "CREATE TABLE users (x INT)");
        insert_values(&mut db, "users", vec![vec![Value::Int64(1)]]);

        let res = exec_sql(&mut db, "SELECT x FROM users LIMIT 0");

        match res {
            QueryResult::Rows(rows) => assert!(rows.is_empty()),
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn limit_resets_on_open() {
        let mut db = test_db();

        exec_sql(&mut db, "CREATE TABLE users (x INT)");
        insert_values(
            &mut db,
            "users",
            vec![vec![Value::Int64(1)], vec![Value::Int64(2)]],
        );

        let r1 = exec_sql(&mut db, "SELECT x FROM users LIMIT 1");
        let r2 = exec_sql(&mut db, "SELECT x FROM users LIMIT 1");

        match (r1, r2) {
            (QueryResult::Rows(a), QueryResult::Rows(b)) => {
                assert_eq!(a.len(), 1);
                assert_eq!(b.len(), 1);
            }
            _ => panic!("expected rows"),
        }
    }
    #[test]
    fn row_can_store_values() {
        let row = Row {
            row_id: Default::default(),
            values: {
                let mut m = HashMap::new();
                m.insert("age".into(), Value::Int64(30));
                m
            },
        };

        assert_eq!(row.values.get("age"), Some(&Value::Int64(30)));
    }
    #[test]
    fn project_selects_columns() {
        let mut db = test_db();

        exec_sql(&mut db, "CREATE TABLE t (name TEXT, age INT)");
        insert_values(
            &mut db,
            "t",
            vec![vec![Value::String("Alice".into()), Value::Int64(30)]],
        );

        let res = exec_sql(&mut db, "SELECT name FROM t");

        match res {
            QueryResult::Rows(rows) => {
                assert_eq!(
                    rows[0].values.get("name"),
                    Some(&Value::String("Alice".into()))
                );
                assert!(rows[0].values.get("age").is_none());
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn project_computes_expressions() {
        let mut db = test_db();

        exec_sql(&mut db, "CREATE TABLE t (age INT)");
        insert_values(&mut db, "t", vec![vec![Value::Int64(20)]]);

        let res = exec_sql(&mut db, "SELECT age + 1 AS next_age FROM t");

        match res {
            QueryResult::Rows(rows) => {
                assert_eq!(rows[0].values.get("next_age"), Some(&Value::Int64(21)));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn scan_returns_all_rows() {
        let mut db = test_db();

        exec_sql(&mut db, "CREATE TABLE t (id INT)");
        insert_values(
            &mut db,
            "t",
            vec![vec![Value::Int64(1)], vec![Value::Int64(2)]],
        );

        let res = exec_sql(&mut db, "SELECT id FROM t");

        match res {
            QueryResult::Rows(rows) => assert_eq!(rows.len(), 2),
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn sort_orders_rows() {
        let mut db = test_db();

        exec_sql(&mut db, "CREATE TABLE t (age INT)");
        insert_values(
            &mut db,
            "t",
            vec![vec![Value::Int64(30)], vec![Value::Int64(10)]],
        );

        let res = exec_sql(&mut db, "SELECT age FROM t ORDER BY age ASC");

        match res {
            QueryResult::Rows(rows) => {
                assert_eq!(rows[0].values.get("age"), Some(&Value::Int64(10)));
            }
            _ => panic!("expected rows"),
        }
    }
}
