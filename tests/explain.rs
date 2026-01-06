mod helpers;

use helpers::harness::TestDB;

use crate::helpers::{data::users_sql, init_debug_for_tests};

#[test]
fn explain_structure() {
    init_debug_for_tests();
    let mut db = TestDB::new();
    db.exec(users_sql()).unwrap();

    let plan = db
        .explain(
            "
        SELECT name
        FROM users
        WHERE age > 18
        LIMIT 5
        ",
        )
        .unwrap();
    println!("{:?}", plan);
    assert!(plan.contains("Limit 5"));
    assert!(plan.contains("Filter"));
    assert!(plan.contains("Scan users"));
}
