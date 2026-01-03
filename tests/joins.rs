mod helpers;

use helium::common::value::Value;
use helpers::{data::*, harness::TestDB};

#[test]
fn inner_join_basic() {
    let mut db = TestDB::new();
    db.exec(users_sql()).unwrap();
    db.exec(orders_sql()).unwrap();

    let rows = db
        .query(
            "
        SELECT u.name, o.amount
        FROM users u
        JOIN orders o ON u.id = o.user_id
        WHERE o.amount > 100
        ",
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values.get("u.name").unwrap(),
        &Value::String("Alice".into())
    );
    assert_eq!(rows[0].values.get("o.amount").unwrap(), &Value::Int64(200));
}
