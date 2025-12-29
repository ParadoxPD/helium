mod helpers;

use helium::common::value::Value;
use helpers::{data::*, harness::TestDB};

#[test]
#[ignore] // GROUP BY + aggregates not implemented yet
fn full_pipeline_query() {
    let mut db = TestDB::new();
    db.register_table("users", users());
    db.register_table("orders", orders());

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
