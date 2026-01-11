mod helpers;

use helium::{
    common::value::Value,
    debugger::{DebugLevel, set_debug_level},
};
use helpers::{data::*, harness::TestDB};

#[test]
fn select_where_limit() {
    let mut db = TestDB::new();
    db.exec(users_sql()).unwrap();

    let rows = db
        .query(
            "
        SELECT name
        FROM users
        WHERE age > 18
        LIMIT 1
        ",
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values.get("name").unwrap(),
        &Value::String("Alice".into())
    );
}

#[test]
fn complex_predicates() {
    let mut db = TestDB::new();

    db.exec(
        "
        CREATE TABLE users (
            id INT,
            name TEXT,
            age INT,
            active BOOL
        );

        INSERT INTO users VALUES (1, 'Alice', 30, true);
        INSERT INTO users VALUES (2, 'Bob', 15, false);
        INSERT INTO users VALUES (3, 'Carol', 40, false);
        ",
    )
    .unwrap();

    let rows = db
        .query(
            "
            SELECT name
            FROM users
            WHERE (age > 18 AND active = true)
               OR name = 'Bob'
            ",
        )
        .unwrap();

    assert_eq!(rows.len(), 2);
}

#[test]
fn multiple_statements_execute() {
    let mut db = TestDB::new();

    db.exec(
        "
        CREATE TABLE t (x INT);
        INSERT INTO t VALUES (1);
        INSERT INTO t VALUES (2);
    ",
    )
    .unwrap();

    let res = db.query("SELECT * FROM t").unwrap();
    assert_eq!(res.len(), 2);
}
