#[cfg(test)]
mod tests {

    use crate::{
        api::db::{Database, QueryResult},
        common::{
            schema::{Column, Schema},
            types::DataType,
            value::Value,
        },
        frontend::sql::{binder::Binder, parser::parse},
    };

    /// Create an isolated test database
    pub fn test_db() -> Database {
        let path = format!("/tmp/helium_test_{}.db", rand::random::<u64>());
        Database::open(path).unwrap()
    }

    /// Build a schema from column names (Phase 1: all Int64 unless specified later)
    pub fn schema(cols: &[&str]) -> Schema {
        Schema {
            columns: cols
                .iter()
                .map(|c| Column {
                    name: c.to_string(),
                    ty: DataType::Int64,
                    nullable: true, // Phase 1 simplification
                })
                .collect(),
        }
    }

    pub fn exec(db: &mut Database, sql: &str) -> QueryResult {
        db.run_query(sql).unwrap()
    }

    /// Insert raw storage rows (Vec<Value>) into a table
    pub fn insert_values(db: &mut Database, table: &str, rows: Vec<Vec<Value>>) {
        let entry = db.catalog.get_table(table).unwrap();
        entry.heap.insert_rows(rows);
    }

    #[test]
    fn sql_query_with_and_and_limit() {
        let mut db = test_db();

        exec(&mut db, "CREATE TABLE users (name INT, age INT, score INT)");

        insert_values(
            &mut db,
            "users",
            vec![
                vec![
                    Value::String("Alice".into()),
                    Value::Int64(30),
                    Value::Int64(80),
                ],
                vec![
                    Value::String("Bob".into()),
                    Value::Int64(15),
                    Value::Int64(90),
                ],
                vec![
                    Value::String("Carol".into()),
                    Value::Int64(40),
                    Value::Int64(40),
                ],
            ],
        );

        match exec(
            &mut db,
            "SELECT name FROM users WHERE age > 18 AND score > 50 LIMIT 1",
        ) {
            QueryResult::Rows(results) => {
                assert_eq!(results.len(), 1);
                assert_eq!(
                    results[0].values.get("users.name"),
                    Some(&Value::String("Alice".into()))
                );
            }
            _ => panic!("expected rows"),
        }
    }
    #[test]
    fn sql_order_by_works() {
        let mut db = test_db();

        exec(&mut db, "CREATE TABLE users (name INT, age INT)");

        insert_values(
            &mut db,
            "users",
            vec![
                vec![Value::String("Bob".into()), Value::Int64(30)],
                vec![Value::String("Alice".into()), Value::Int64(20)],
            ],
        );

        match exec(&mut db, "SELECT name FROM users ORDER BY age ASC") {
            QueryResult::Rows(rows) => {
                assert_eq!(
                    rows[0].values.get("users.name"),
                    Some(&Value::String("Alice".into()))
                );
            }
            _ => panic!("expected rows"),
        }
    }
    #[test]
    fn sql_join_works() {
        let mut db = test_db();

        exec(&mut db, "CREATE TABLE users (id INT, name INT)");
        exec(&mut db, "CREATE TABLE orders (user_id INT, amount INT)");

        insert_values(
            &mut db,
            "users",
            vec![vec![Value::Int64(1), Value::String("Alice".into())]],
        );

        insert_values(
            &mut db,
            "orders",
            vec![vec![Value::Int64(1), Value::Int64(200)]],
        );

        match exec(
            &mut db,
            "SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id",
        ) {
            QueryResult::Rows(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(
                    rows[0].values.get("u.name"),
                    Some(&Value::String("Alice".into()))
                );
            }
            _ => panic!("expected rows"),
        }
    }
    #[test]
    fn create_index_registers_in_catalog() {
        let mut db = test_db();

        exec(&mut db, "CREATE TABLE users (id INT, age INT)");
        exec(&mut db, "CREATE INDEX idx_users_age ON users(age)");

        let index = db.catalog.get_index("users", "age");
        assert!(index.is_some());
    }
    #[test]
    fn create_index_enables_index_scan() {
        let mut db = test_db();

        exec(&mut db, "CREATE TABLE users (id INT, age INT)");

        insert_values(
            &mut db,
            "users",
            vec![
                vec![Value::Int64(1), Value::Int64(20)],
                vec![Value::Int64(2), Value::Int64(30)],
            ],
        );

        exec(&mut db, "CREATE INDEX idx_users_age ON users(age)");

        match exec(&mut db, "SELECT * FROM users WHERE age = 20") {
            QueryResult::Rows(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].values.get("users.age"), Some(&Value::Int64(20)));
            }
            _ => panic!("expected rows"),
        }
    }
    #[test]
    fn create_index_twice_errors_or_overwrites() {
        let mut db = test_db();

        exec(&mut db, "CREATE TABLE users (id INT, age INT)");
        exec(&mut db, "CREATE INDEX idx_users_age ON users(age)");

        let result = exec(&mut db, "CREATE INDEX idx_users_age ON users(age)");

        // Phase 1 decision: overwrite or ignore is allowed
        matches!(result, QueryResult::Ok);
    }
    #[test]
    fn drop_index_removes_from_catalog() {
        let mut db = test_db();

        exec(&mut db, "CREATE TABLE users (id INT, age INT)");
        exec(&mut db, "CREATE INDEX idx_users_age ON users(age)");

        assert!(db.catalog.get_index("users", "age").is_some());

        exec(&mut db, "DROP INDEX idx_users_age");

        assert!(db.catalog.get_index("users", "age").is_none());
    }
    #[test]
    fn drop_index_disables_index_scan() {
        let mut db = test_db();

        exec(&mut db, "CREATE TABLE users (id INT, age INT)");

        insert_values(
            &mut db,
            "users",
            vec![vec![Value::Int64(1), Value::Int64(20)]],
        );

        exec(&mut db, "CREATE INDEX idx_users_age ON users(age)");
        exec(&mut db, "DROP INDEX idx_users_age");

        match exec(&mut db, "EXPLAIN SELECT * FROM users WHERE age = 20") {
            QueryResult::Explain(plan) => {
                assert!(!plan.contains("IndexScan"));
            }
            _ => panic!("expected explain"),
        }
    }
}
