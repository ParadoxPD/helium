#[cfg(test)]
mod tests {
    use maplit::hashmap;

    use super::*;
    use crate::common::value::Value;

    fn row(pairs: &[(&str, Value)]) -> Row {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn sql_query_with_and_and_limit() {
        let mut db = Database::new();

        let schema = vec!["name".into(), "age".into(), "score".into()];

        db.insert_table(
            "users",
            schema,
            vec![
                row(&[
                    ("name", Value::String("Alice".into())),
                    ("age", Value::Int64(30)),
                    ("score", Value::Int64(80)),
                ]),
                row(&[
                    ("name", Value::String("Bob".into())),
                    ("age", Value::Int64(15)),
                    ("score", Value::Int64(90)),
                ]),
                row(&[
                    ("name", Value::String("Carol".into())),
                    ("age", Value::Int64(40)),
                    ("score", Value::Int64(40)),
                ]),
            ],
        );

        match db.query("SELECT name FROM users WHERE age > 18 AND score > 50 LIMIT 1;") {
            QueryResult::Rows(results) => {
                assert_eq!(results.len(), 1);
                assert_eq!(results[0].get("name"), Some(&Value::String("Alice".into())));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn sql_order_by_works() {
        let mut db = Database::new();

        let schema = vec!["name".into(), "age".into()];

        db.insert_table(
            "users",
            schema,
            vec![
                row(&[
                    ("name", Value::String("Bob".into())),
                    ("age", Value::Int64(30)),
                ]),
                row(&[
                    ("name", Value::String("Alice".into())),
                    ("age", Value::Int64(20)),
                ]),
            ],
        );

        match db.query("SELECT name FROM users ORDER BY age ASC;") {
            QueryResult::Rows(rows) => {
                assert_eq!(rows[0].get("name"), Some(&Value::String("Alice".into())));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn sql_join_works() {
        let mut db = Database::new();

        let users_schema = vec!["id".into(), "name".into()];

        let orders_schema = vec!["user_id".into(), "amount".into()];

        db.insert_table(
            "users",
            users_schema,
            vec![row(&[
                ("id", Value::Int64(1)),
                ("name", Value::String("Alice".into())),
            ])],
        );

        db.insert_table(
            "orders",
            orders_schema,
            vec![row(&[
                ("user_id", Value::Int64(1)),
                ("amount", Value::Int64(200)),
            ])],
        );

        match db.query("SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id;") {
            QueryResult::Rows(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get("name"), Some(&Value::String("Alice".into())));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn create_index_registers_in_catalog() {
        let mut db = Database::new();

        db.insert_table("users", vec!["id".into(), "age".into()], vec![]);

        db.query("CREATE INDEX idx_users_age ON users(age)");

        let index = db.catalog.get_index("users", "age");

        assert!(index.is_some());
    }

    #[test]
    fn create_index_enables_index_scan() {
        let mut db = Database::new();
        let rows = vec![
            hashmap! { "id".into() => Value::Int64(1), "age".into() => Value::Int64(20) },
            hashmap! { "id".into() => Value::Int64(2), "age".into() => Value::Int64(30) },
        ];
        println!("{:?}", rows);

        db.insert_table("users", vec!["id".into(), "age".into()], rows);

        let res = db.query("CREATE INDEX idx_users_age ON users(age)");
        println!("{:?}", res);

        let result = db.query("SELECT * FROM users WHERE age = 20");

        match result {
            QueryResult::Rows(rows) => {
                assert_eq!(rows.len(), 1);
                println!("RETURNED ROWS = {:?}", rows);
                assert_eq!(rows[0]["age"], Value::Int64(20));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn create_index_twice_errors_or_overwrites() {
        let mut db = Database::new();

        db.insert_table("users", vec!["id".into(), "age".into()], vec![]);

        db.query("CREATE INDEX idx_users_age ON users(age)");

        // Second create — choose ONE behavior:
        // either panic OR overwrite OR return error
        let result = db.query("CREATE INDEX idx_users_age ON users(age)");

        // If you choose overwrite or ignore:
        matches!(result, QueryResult::Explain(_));
    }

    #[test]
    fn drop_index_removes_from_catalog() {
        let mut db = Database::new();

        db.insert_table("users", vec!["id".into(), "age".into()], vec![]);

        db.query("CREATE INDEX idx_users_age ON users(age)");
        assert!(db.catalog.get_index("users", "age").is_some());

        db.query("DROP INDEX idx_users_age");
        assert!(db.catalog.get_index("users", "age").is_none());
    }

    #[test]
    fn drop_index_disables_index_scan() {
        let mut db = Database::new();

        db.insert_table(
            "users",
            vec!["id".into(), "age".into()],
            vec![hashmap! { "id".into() => Value::Int64(1), "age".into() => Value::Int64(20) }],
        );

        db.query("CREATE INDEX idx_users_age ON users(age)");
        db.query("DROP INDEX idx_users_age");

        let plan = match db.query("EXPLAIN SELECT * FROM users WHERE age = 20") {
            QueryResult::Explain(e) => e,
            _ => panic!(),
        };

        assert!(!plan.contains("IndexScan"));
    }
}
