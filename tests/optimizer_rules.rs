mod helpers;

use helpers::harness::TestDB;

use crate::helpers::data::users_sql;

#[test]
fn projection_pruning_removes_redundant_projects() {
    let mut db = TestDB::new();
    println!("EXEC {:?}", db.exec(users_sql()));

    let plan = db
        .explain(
            "
        SELECT name
        FROM users
        WHERE age > 18
    ",
        )
        .unwrap();

    assert!(!plan.contains("Project [name, age]"));
    assert!(plan.contains("Scan users"));
}
