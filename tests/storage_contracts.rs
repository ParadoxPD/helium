mod helpers;

use helpers::{data::users, harness::TestDB};

#[test]
#[ignore = "unlocked when disk storage exists"]
fn disk_and_memory_storage_match() {
    let mut db = TestDB::new();
    db.register_table("users", users());

    let mem_rows = db.query("SELECT * FROM users");

    // later: switch to disk-backed table
    let disk_rows = mem_rows.clone();

    assert_eq!(mem_rows, disk_rows);
}
