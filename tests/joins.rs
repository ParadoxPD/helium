mod helpers;

use helium::common::value::Value;
use helpers::{data::*, harness::TestDB};

#[test]
fn inner_join_basic() {
    let mut db = TestDB::new();
    db.register_table("users", users_schema(), users());
    db.register_table("orders", orders_schema(), orders());

    let rows = db.query(
        "
        SELECT u.name, o.amount
        FROM users u
        JOIN orders o ON u.id = o.user_id
        WHERE o.amount > 100
    ",
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["name"], Value::String("Alice".into()));
    assert_eq!(rows[0]["amount"], Value::Int64(200));
}
