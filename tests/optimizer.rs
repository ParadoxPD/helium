mod helpers;

use helpers::harness::TestDB;

#[test]
fn projection_pruning_removes_redundant_projects() {
    let db = TestDB::new();

    let plan = db.explain(
        "
        SELECT name
        FROM users
        WHERE age > 18
    ",
    );

    assert!(!plan.contains("Project [name, age]"));
    assert!(plan.contains("Scan users"));
}

