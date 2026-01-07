mod helpers;

use helium::common::value::Value;
use helpers::{data::*, harness::TestDB};

#[test]
fn full_pipeline_query() {
    let mut db = TestDB::new();
    db.exec(users_sql()).unwrap();
    db.exec(orders_sql()).unwrap();

    let rows = db
        .query(
            "
        SELECT u.name
        FROM users u
        JOIN orders o ON u.id = o.user_id
        WHERE o.amount > 50
        ORDER BY o.amount DESC
        LIMIT 1
        ",
        )
        .unwrap();

    assert_eq!(
        rows[0].values.get("name").unwrap(),
        &Value::String("Alice".into())
    );
}

#[test]
fn create_table_then_select() {
    let mut db = TestDB::new();

    db.exec("CREATE TABLE users (id INT, name TEXT)").unwrap();
    let rows = db.query("SELECT * FROM users").unwrap();

    assert!(rows.is_empty());
}

#[test]
fn drop_missing_table_fails() {
    let mut db = TestDB::new();
    assert!(db.exec("DROP TABLE nope").is_err());
}

#[test]
fn drop_table_removes_it() {
    let mut db = TestDB::new();

    db.exec("CREATE TABLE users (id INT)").unwrap();
    db.exec("DROP TABLE users").unwrap();

    assert!(db.query("SELECT * FROM users").is_err());
}

#[test]
fn update_single_row() {
    let mut db = TestDB::new();

    db.exec("CREATE TABLE users (id INT, name TEXT)").unwrap();
    db.exec("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    db.exec("UPDATE users SET name = 'Bob' WHERE id = 1")
        .unwrap();

    let rows = db.query("SELECT name FROM users WHERE id = 1").unwrap();
    assert_eq!(
        rows[0].values.get("name").unwrap(),
        &Value::String("Bob".into())
    );
}

#[test]
fn update_where_no_match() {
    let mut db = TestDB::new();
    db.exec("CREATE TABLE users (id INT, name TEXT)").unwrap();
    db.exec("UPDATE users SET name = 'Z' WHERE id = 999")
        .unwrap();
}

#[test]
fn update_type_error() {
    let mut db = TestDB::new();
    db.exec("CREATE TABLE users (id INT)").unwrap();
    assert!(db.exec("UPDATE users SET id = 'abc'").is_err());
}

#[test]
fn insert_row() {
    let mut db = TestDB::new();
    db.exec("CREATE TABLE users (id INT, name TEXT)").unwrap();
    db.exec("INSERT INTO users VALUES (1, 'Alice')").unwrap();

    let rows = db.query("SELECT * FROM users").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn insert_column_mismatch() {
    let mut db = TestDB::new();
    db.exec("CREATE TABLE t (a INT, b INT)").unwrap();
    assert!(db.exec("INSERT INTO t VALUES (1)").is_err());
}

#[test]
fn delete_where() {
    let mut db = TestDB::new();
    db.exec("CREATE TABLE t (a INT)").unwrap();
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    db.exec("INSERT INTO t VALUES (2)").unwrap();

    println!("{:?}", db.exec("DELETE FROM t WHERE a = 1"));

    let rows = db.query("SELECT * FROM t").unwrap();
    println!("{:?}", rows);
    assert_eq!(rows.len(), 1);
}

#[test]
fn delete_all() {
    let mut db = TestDB::new();
    db.exec("CREATE TABLE t (a INT)").unwrap();
    db.exec("INSERT INTO t VALUES (1)").unwrap();

    db.exec("DELETE FROM t").unwrap();
    assert!(db.query("SELECT * FROM t").unwrap().is_empty());
}
