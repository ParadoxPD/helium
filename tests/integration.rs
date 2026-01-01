mod helpers;

use helium::common::value::Value;
use helpers::{data::*, harness::TestDB};

#[test]
#[ignore = "GROUP BY + aggregates not implemented yet"]
fn full_pipeline_query() {
    let mut db = TestDB::new();
    db.register_table("users", users_schema(), users());
    db.register_table("orders", orders_schema(), orders());

    let rows = db.query(
        "
        SELECT u.name
        FROM users u
        JOIN orders o ON u.id = o.user_id
        WHERE o.amount > 50
        ORDER BY o.amount DESC
        LIMIT 1
    ",
    );

    assert_eq!(rows[0]["name"], Value::String("Alice".into()));
}

#[test]
fn create_table_then_select() {
    let db = TestDB::new();

    db.exec("CREATE TABLE users (id INT, name TEXT)");
    let rows = db.query("SELECT * FROM users");

    assert!(rows.is_empty());
}

#[test]
fn drop_missing_table_fails() {
    let db = TestDB::new();
    assert!(db.exec("DROP TABLE nope").is_err());
}

#[test]
fn drop_table_removes_it() {
    let db = TestDB::new();

    db.exec("CREATE TABLE users (id INT)");
    db.exec("DROP TABLE users");

    assert!(db.query("SELECT * FROM users").is_err());
}

#[test]
fn update_single_row() {
    let db = TestDB::new();

    db.exec("CREATE TABLE users (id INT, name TEXT)");
    db.exec("INSERT INTO users VALUES (1, 'Alice')");
    db.exec("UPDATE users SET name = 'Bob' WHERE id = 1");

    let rows = db.query("SELECT name FROM users WHERE id = 1");
    assert_eq!(rows[0]["name"], "Bob");
}

#[test]
fn update_all_rows() {
    db.exec("UPDATE users SET name = 'X'");
}

#[test]
fn update_type_error() {
    assert!(db.exec("UPDATE users SET id = 'abc'").is_err());
}

#[test]
fn update_where_no_match() {
    db.exec("UPDATE users SET name = 'Z' WHERE id = 999");
}

#[test]
fn insert_row() {
    let db = TestDB::new();
    db.exec("CREATE TABLE users (id INT, name TEXT)");
    db.exec("INSERT INTO users VALUES (1, 'Alice')");

    let rows = db.query("SELECT * FROM users");
    assert_eq!(rows.len(), 1);
}

#[test]
fn insert_column_mismatch() {
    let db = TestDB::new();
    db.exec("CREATE TABLE t (a INT, b INT)");
    assert!(db.exec("INSERT INTO t VALUES (1)").is_err());
}

#[test]
fn insert_type_error() {
    let db = TestDB::new();
    db.exec("CREATE TABLE t (a INT)");
    assert!(db.exec("INSERT INTO t VALUES ('x')").is_err());
}

#[test]
fn insert_null() {
    let db = TestDB::new();
    db.exec("CREATE TABLE t (a INT)");
    db.exec("INSERT INTO t VALUES (NULL)");

    let rows = db.query("SELECT * FROM t");
    assert!(rows[0]["a"].is_null());
}

#[test]
fn delete_where() {
    let db = TestDB::new();
    db.exec("CREATE TABLE t (a INT)");
    db.exec("INSERT INTO t VALUES (1)");
    db.exec("INSERT INTO t VALUES (2)");

    db.exec("DELETE FROM t WHERE a = 1");

    let rows = db.query("SELECT * FROM t");
    assert_eq!(rows.len(), 1);
}

#[test]
fn delete_all() {
    db.exec("DELETE FROM t");
    assert!(db.query("SELECT * FROM t").is_empty());
}

#[test]
fn delete_no_match() {
    db.exec("DELETE FROM t WHERE a = 999");
}
