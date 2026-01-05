mod helpers;

use helpers::harness::TestDB;

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
