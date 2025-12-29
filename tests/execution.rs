mod helpers;

use helpers::{data::*, harness::TestDB};

#[test]
#[ignore = "needs execution stats"]
fn limit_short_circuits_execution() {
    let mut db = TestDB::new();
    db.register_table("users", users());

    let stats = db.explain_analyze(
        "
        SELECT *
        FROM users
        LIMIT 1
    ",
    );

    assert!(stats.contains("rows=1"));
}
