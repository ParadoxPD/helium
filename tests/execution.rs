mod helpers;

use helpers::{data::*, harness::TestDB};

#[test]
#[ignore = "needs execution stats"]
fn limit_short_circuits_execution() {
    let mut db = TestDB::new();
    db.exec(users_sql()).unwrap();

    let stats = db.explain_analyze(
        "
        SELECT *
        FROM users
        LIMIT 1
        ",
    );

    assert!(stats.contains("rows=1"));
}
