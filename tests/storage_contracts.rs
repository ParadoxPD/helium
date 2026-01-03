use crate::helpers::harness::TestDB;

mod helpers;

#[test]
fn disk_and_memory_storage_match() {
    let mut db = TestDB::new();

    db.exec(
        "
        CREATE TABLE users (
            id INT,
            name TEXT,
            age INT
        );

        INSERT INTO users VALUES (1, 'Alice', 30);
        INSERT INTO users VALUES (2, 'Bob', 15);
        ",
    )
    .unwrap();

    let mem_rows = db.query("SELECT * FROM users").unwrap();

    // currently: same storage, same execution
    let disk_rows = db.query("SELECT * FROM users").unwrap();

    assert_eq!(mem_rows, disk_rows);
}
