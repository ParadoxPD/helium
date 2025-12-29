mod helpers;

use helium::common::value::Value;
use helpers::{data::*, harness::TestDB};

#[test]
fn select_where_limit() {
    let mut db = TestDB::new();
    db.register_table("users", users());

    let rows = db.query(
        "
        SELECT name
        FROM users
        WHERE age > 18
        LIMIT 1
    ",
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["name"], Value::String("Alice".into()));
}

#[test]
#[ignore] // AND / OR / parentheses not fully done yet
fn complex_predicates() {
    let mut db = TestDB::new();
    db.register_table("users", users());

    let rows = db.query(
        "
        SELECT name
        FROM users
        WHERE (age > 18 AND active = true)
           OR name = 'Bob'
    ",
    );

    assert_eq!(rows.len(), 2);
}
