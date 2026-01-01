#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::{
        buffer::buffer_pool::BufferPool,
        common::value::Value,
        exec::{filter::FilterExec, operator::Operator, scan::ScanExec},
        ir::expr::{BinaryOp, Expr},
        storage::{
            page::{PageId, RowId, StorageRow},
            page_manager::FilePageManager,
            table::{HeapTable, Table},
        },
    };

    #[test]
    fn filter_removes_rows() {
        let schema = vec!["id".into(), "age".into()];

        let rows = vec![
            vec![Value::Int64(1), Value::Int64(10)],
            vec![Value::Int64(2), Value::Int64(30)],
        ];

        let path = format!("/tmp/db_{}.db", rand::random::<u64>());

        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        // -------- create table --------
        let mut table = HeapTable::new("users".into(), schema.clone(), 4, bp.clone());
        table.insert_rows(rows);

        let scan = ScanExec::new(Arc::new(table), "users".into(), schema);

        let predicate = Expr::bin(
            Expr::bound_col("users", "age"),
            BinaryOp::Gt,
            Expr::lit(Value::Int64(18)),
        );

        let mut filter = FilterExec::new(Box::new(scan), predicate);
        filter.open();

        let row = filter.next().unwrap();
        assert_eq!(row.get("users.age"), Some(&Value::Int64(30)));
        assert!(filter.next().is_none());
    }

    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::buffer::buffer_pool::BufferPool;
    use crate::common::value::Value;
    use crate::exec::operator::Operator;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::ir::plan::{IndexPredicate, LogicalPlan};
    use crate::storage::btree::DiskBPlusTree;
    use crate::storage::btree::node::IndexKey;
    use crate::storage::page_manager::FilePageManager;
    use crate::storage::table::HeapTable;

    #[test]
    fn execute_simple_scan() {
        let schema = vec!["id confirmed".into(), "name".into(), "age".into()];

        let rows = vec![
            vec![Value::Int64(1), Value::Null, Value::Null],
            vec![Value::Int64(2), Value::Null, Value::Null],
        ];

        let path = format!("/tmp/db_{}.db", rand::random::<u64>());
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let mut table = HeapTable::new("users".into(), schema.clone(), 4, bp.clone());
        table.insert_rows(rows);

        let mut catalog = Catalog::new();
        catalog.insert("users".into(), Arc::new(table));

        let plan = LogicalPlan::scan("users", "u");
        let mut exec = lower(&plan, &catalog);

        exec.open();
        assert!(exec.next().is_some());
        assert!(exec.next().is_some());
        assert!(exec.next().is_none());
    }

    #[test]
    fn execute_filter_project_limit() {
        let schema = vec!["name".into(), "age".into()];

        let rows = vec![
            vec![Value::String("Alice".into()), Value::Int64(30)],
            vec![Value::String("Bob".into()), Value::Int64(15)],
            vec![Value::String("Carol".into()), Value::Int64(40)],
        ];

        let path = format!("/tmp/db_{}.db", rand::random::<u64>());
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let mut table = HeapTable::new("users".into(), schema.clone(), 4, bp.clone());
        table.insert_rows(rows);

        let mut catalog = Catalog::new();
        catalog.insert("users".into(), Arc::new(table));

        let plan = LogicalPlan::scan("users", "u")
            .filter(Expr::bin(
                Expr::bound_col("u", "age"),
                BinaryOp::Gt,
                Expr::lit(Value::Int64(18)),
            ))
            .project(vec![(Expr::bound_col("u", "name"), "name")])
            .limit(2);

        let mut exec = lower(&plan, &catalog);
        exec.open();

        let r1 = exec.next().unwrap();
        let r2 = exec.next().unwrap();

        assert_eq!(r1.get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(r2.get("name"), Some(&Value::String("Carol".into())));
        assert!(exec.next().is_none());
    }

    #[test]
    fn execution_respects_optimizer_output() {
        let schema = vec!["x".into()];

        let rows = vec![
            vec![Value::Int64(1)],
            vec![Value::Int64(2)],
            vec![Value::Int64(3)],
        ];

        let path = format!("/tmp/db_{}.db", rand::random::<u64>());
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let mut table = HeapTable::new("users".into(), schema.clone(), 4, bp.clone());
        table.insert_rows(rows);

        let mut catalog = Catalog::new();
        catalog.insert("users".into(), Arc::new(table));

        let plan = LogicalPlan::scan("users", "u")
            .filter(Expr::bin(
                Expr::bound_col("u", "x"),
                BinaryOp::Gt,
                Expr::lit(Value::Int64(1)),
            ))
            .limit(1);

        let mut exec = lower(&plan, &catalog);
        exec.open();

        let row = exec.next().unwrap();
        assert_eq!(row.get("u.x"), Some(&Value::Int64(2)));
        assert!(exec.next().is_none());
    }

    #[test]
    fn index_scan_end_to_end() {
        let path = format!("/tmp/db_{}.db", rand::random::<u64>());

        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let mut table = HeapTable::new("t".into(), vec!["id".into()], 4, bp.clone());
        let mut index = DiskBPlusTree::new(4, bp.clone());

        for i in 0..10 {
            let rid = table.insert(vec![Value::Int64(i)]);
            index.insert(IndexKey::Int64(i), rid);
        }

        let table = Arc::new(table);
        let index = Arc::new(Mutex::new(index));

        let mut exec = IndexScanExec::new(
            table.clone(),
            "t".into(),
            index,
            IndexPredicate::Eq(Value::Int64(5)),
            "id".into(),
            table.schema().to_vec(),
        );

        exec.open();
        let row = exec.next().unwrap();

        assert_eq!(row.get("t.id"), Some(&Value::Int64(5)));
        assert!(exec.next().is_none());
    }

    use crate::buffer::buffer_pool::BufferPool;
    use crate::common::value::Value;
    use crate::exec::index_scan::IndexScanExec;
    use crate::exec::operator::Operator;
    use crate::ir::plan::IndexPredicate;
    use crate::storage::btree::DiskBPlusTree;
    use crate::storage::btree::node::{Index, IndexKey};
    use crate::storage::page_manager::FilePageManager;
    use crate::storage::table::{HeapTable, Table};
    use std::sync::{Arc, Mutex};

    #[test]
    fn index_scan_exec_returns_matching_rows() {
        let path = format!("/tmp/db_{}.db", rand::random::<u64>());

        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        // -------- create table --------
        let mut table = HeapTable::new(
            "users".into(),
            vec!["id".into(), "age".into()],
            4,
            bp.clone(),
        );

        // -------- create index --------
        let mut index = DiskBPlusTree::new(4, bp.clone());

        // -------- insert rows + index entries --------
        // We want age = 20 to appear twice
        let rows = vec![
            vec![Value::Int64(1), Value::Int64(20)],
            vec![Value::Int64(2), Value::Int64(20)],
            vec![Value::Int64(3), Value::Int64(30)],
            vec![Value::Int64(4), Value::Int64(40)],
        ];

        for row in rows {
            let rid = table.insert(row.clone());

            // index on "age" column (index key = row[1])
            let age = match &row[1] {
                Value::Int64(v) => *v,
                _ => unreachable!(),
            };

            index.insert(IndexKey::Int64(age), rid);
        }

        let table: Arc<dyn Table> = Arc::new(table);
        let index: Arc<Mutex<dyn Index>> = Arc::new(Mutex::new(index));

        // -------- run index scan --------
        let mut exec = IndexScanExec::new(
            table.clone(),
            "users".into(),
            index.clone(),
            IndexPredicate::Eq(Value::Int64(20)),
            "age".into(),
            table.schema().to_vec(),
        );

        exec.open();

        let mut rows = Vec::new();
        while let Some(row) = exec.next() {
            rows.push(row);
        }

        // -------- assertions --------
        assert_eq!(rows.len(), 2);

        for r in rows {
            assert_eq!(r["users.age"], Value::Int64(20));
        }
    }

    use crate::buffer::buffer_pool::BufferPool;
    use crate::common::value::Value;
    use crate::exec::join::JoinExec;
    use crate::exec::operator::Operator;
    use crate::exec::scan::ScanExec;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::storage::page::PageId;
    use crate::storage::page::{RowId, StorageRow};
    use crate::storage::page_manager::FilePageManager;
    use crate::storage::table::HeapTable;
    use std::sync::{Arc, Mutex};

    #[test]
    fn join_matches_rows() {
        // ---------- schema ----------
        let user_schema = vec!["id".into(), "name".into()];
        let order_schema = vec!["user_id".into(), "amount".into()];

        let path = format!("/tmp/db_{}.db", rand::random::<u64>());
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        // ---------- left table (users) ----------
        let mut users_table = HeapTable::new("users".into(), user_schema.clone(), 4, bp.clone());

        users_table.insert_rows(vec![
            vec![Value::Int64(1), Value::String("Alice".into())],
            vec![Value::Int64(2), Value::String("Bob".into())],
        ]);

        // ---------- right table (orders) ----------
        let mut orders_table = HeapTable::new("orders".into(), order_schema.clone(), 4, bp.clone());

        orders_table.insert_rows(vec![
            vec![Value::Int64(1), Value::Int64(200)],
            vec![Value::Int64(3), Value::Int64(50)],
        ]);

        // ---------- scans ----------
        let left = ScanExec::new(Arc::new(users_table), "u".into(), user_schema);
        let right = ScanExec::new(Arc::new(orders_table), "o".into(), order_schema);

        // ---------- join condition ----------
        let on = Expr::bin(
            Expr::bound_col("u", "id"),
            BinaryOp::Eq,
            Expr::bound_col("o", "user_id"),
        );

        let mut join = JoinExec::new(Box::new(left), Box::new(right), on);
        join.open();

        let result = join.next().unwrap();

        assert_eq!(result.get("u.name"), Some(&Value::String("Alice".into())));
        assert_eq!(result.get("o.amount"), Some(&Value::Int64(200)));

        assert!(join.next().is_none());
    }

    use crate::buffer::buffer_pool::BufferPool;
    use crate::common::value::Value;
    use crate::exec::limit::LimitExec;
    use crate::exec::operator::Operator;
    use crate::exec::scan::ScanExec;
    use crate::storage::page::{PageId, RowId, StorageRow};
    use crate::storage::page_manager::FilePageManager;
    use crate::storage::table::{HeapTable, Table};

    fn rows(vals: &[i64]) -> Vec<Vec<Value>> {
        vals.iter()
            .enumerate()
            .map(|(_, v)| vec![Value::Int64(*v)])
            .collect()
    }

    #[test]
    fn limit_returns_only_n_rows() {
        let schema = vec!["x".into()];
        let path = format!("/tmp/db_{}.db", rand::random::<u64>());

        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        // -------- create table --------
        let mut table = HeapTable::new("users".into(), schema.clone(), 4, bp.clone());

        table.insert_rows(rows(&[1, 2, 3]));

        let scan = ScanExec::new(Arc::new(table), "t".into(), schema);
        let mut limit = LimitExec::new(Box::new(scan), 2);

        limit.open();

        assert!(limit.next().is_some());
        assert!(limit.next().is_some());
        assert!(limit.next().is_none());
    }

    #[test]
    fn limit_zero_returns_no_rows() {
        let schema = vec!["x".into()];
        let path = format!("/tmp/db_{}.db", rand::random::<u64>());

        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        // -------- create table --------
        let mut table = HeapTable::new("users".into(), schema.clone(), 4, bp.clone());

        table.insert_rows(rows(&[1]));

        let scan = ScanExec::new(Arc::new(table), "t".into(), schema);
        let mut limit = LimitExec::new(Box::new(scan), 0);

        limit.open();
        assert!(limit.next().is_none());
    }

    #[test]
    fn limit_does_not_consume_extra_rows() {
        let schema = vec!["x".into()];
        let path = format!("/tmp/db_{}.db", rand::random::<u64>());

        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        // -------- create table --------
        let mut table = HeapTable::new("users".into(), schema.clone(), 4, bp.clone());

        table.insert(vec![Value::Int64(1)]);
        let scan = ScanExec::new(Arc::new(table), "t".into(), schema);
        let mut limit = LimitExec::new(Box::new(scan), 1);

        limit.open();
        let first = limit.next().unwrap();
        assert_eq!(first.get("t.x"), Some(&Value::Int64(1)));

        assert!(limit.next().is_none());
    }

    #[test]
    fn limit_resets_on_open() {
        let schema = vec!["x".into()];
        let path = format!("/tmp/db_{}.db", rand::random::<u64>());

        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        // -------- create table --------
        let mut table = HeapTable::new("users".into(), schema.clone(), 4, bp.clone());

        table.insert_rows(rows(&[1, 2]));

        let scan = ScanExec::new(Arc::new(table), "t".into(), schema);
        let mut limit = LimitExec::new(Box::new(scan), 1);

        limit.open();
        assert!(limit.next().is_some());
        assert!(limit.next().is_none());

        // reopen
        limit.open();
        assert!(limit.next().is_some());
    }
    use super::*;

    #[test]
    fn row_can_store_values() {
        let mut row = Row::new();
        row.insert("age".into(), Value::Int64(30));

        assert_eq!(row.get("age"), Some(&Value::Int64(30)));
    }

    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::buffer::buffer_pool::BufferPool;
    use crate::common::value::Value;
    use crate::exec::operator::Operator;
    use crate::exec::scan::ScanExec;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::storage::page_manager::FilePageManager;
    use crate::storage::table::HeapTable;

    #[test]
    fn project_selects_columns() {
        let schema = vec!["name".into(), "age".into()];
        let rows = vec![vec![Value::String("Alice".into()), Value::Int64(30)]];

        let path = format!("/tmp/db_{}.db", rand::random::<u64>());
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let mut table = HeapTable::new("t".into(), schema.clone(), 4, bp);
        table.insert_rows(rows);

        let scan = ScanExec::new(Arc::new(table), "t".into(), schema);

        let mut project = ProjectExec::new(
            Box::new(scan),
            vec![(
                Expr::BoundColumn {
                    table: "t".into(),
                    name: "name".into(),
                },
                "name".into(),
            )],
        );

        project.open();
        let out = project.next().unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out.get("name"), Some(&Value::String("Alice".into())));
    }

    #[test]
    fn project_computes_expressions() {
        let schema = vec!["age".into()];
        let rows = vec![vec![Value::Int64(20)]];

        let path = format!("/tmp/db_{}.db", rand::random::<u64>());
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let mut table = HeapTable::new("t".into(), schema.clone(), 4, bp);
        table.insert_rows(rows);

        let scan = ScanExec::new(Arc::new(table), "t".into(), schema);

        let expr = Expr::bin(
            Expr::bound_col("t", "age"),
            BinaryOp::Add,
            Expr::lit(Value::Int64(1)),
        );

        let mut project = ProjectExec::new(Box::new(scan), vec![(expr, "next_age".into())]);

        project.open();
        let out = project.next().unwrap();

        assert_eq!(out.get("next_age"), Some(&Value::Int64(21)));
    }

    #[test]
    fn project_handles_missing_column_as_null() {
        let schema = vec!["name".into()];
        let rows = vec![vec![Value::String("Bob".into())]];

        let path = format!("/tmp/db_{}.db", rand::random::<u64>());
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let mut table = HeapTable::new("t".into(), schema.clone(), 4, bp);
        table.insert_rows(rows);

        let scan = ScanExec::new(Arc::new(table), "t".into(), schema);

        let mut project = ProjectExec::new(
            Box::new(scan),
            vec![(Expr::bound_col("t", "age"), "age".into())],
        );

        project.open();
        let out = project.next().unwrap();

        assert_eq!(out.get("age"), Some(&Value::Null));
    }

    #[test]
    fn project_preserves_row_count() {
        let schema = vec!["x".into()];
        let rows = vec![vec![Value::Int64(1)], vec![Value::Int64(2)]];

        let path = format!("/tmp/db_{}.db", rand::random::<u64>());
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let mut table = HeapTable::new("t".into(), schema.clone(), 4, bp);
        table.insert_rows(rows);

        let scan = ScanExec::new(Arc::new(table), "t".into(), schema);

        let mut project = ProjectExec::new(
            Box::new(scan),
            vec![(Expr::bound_col("t", "x"), "x".into())],
        );

        project.open();
        assert!(project.next().is_some());
        assert!(project.next().is_some());
        assert!(project.next().is_none());
    }
    use std::sync::{Arc, Mutex};

    use crate::{
        buffer::buffer_pool::BufferPool,
        common::value::Value,
        exec::{operator::Operator, scan::ScanExec},
        storage::{
            page_manager::FilePageManager,
            table::{HeapTable, Table},
        },
    };

    #[test]
    fn scan_returns_all_rows() {
        let schema = vec!["id".into()];
        let rows = vec![vec![Value::Int64(1)], vec![Value::Int64(2)]];

        let path = format!("/tmp/db_{}.db", rand::random::<u64>());
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let mut table = HeapTable::new("t".into(), schema.clone(), 4, bp);
        table.insert_rows(rows);

        let mut scan = ScanExec::new(Arc::new(table), "t".into(), schema);
        scan.open();

        assert!(scan.next().is_some());
        assert!(scan.next().is_some());
        assert!(scan.next().is_none());
    }

    #[test]
    fn table_cursor_emits_distinct_row_ids() {
        let schema = vec!["id".into()];
        let rows = vec![vec![Value::Int64(1)], vec![Value::Int64(2)]];

        let path = format!("/tmp/db_{}.db", rand::random::<u64>());
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let mut table = HeapTable::new("users".into(), schema.clone(), 4, bp);
        table.insert_rows(rows);

        let table = Arc::new(table);
        let mut cursor = table.scan();

        let r1 = cursor.next().unwrap();
        let r2 = cursor.next().unwrap();

        assert_ne!(r1.rid, r2.rid);
    }
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::buffer::buffer_pool::BufferPool;
    use crate::common::value::Value;
    use crate::exec::operator::Operator;
    use crate::exec::scan::ScanExec;
    use crate::ir::expr::Expr;
    use crate::storage::page_manager::FilePageManager;
    use crate::storage::table::HeapTable;

    #[test]
    fn sort_orders_rows() {
        let schema = vec!["age".into()];
        let rows = vec![vec![Value::Int64(30)], vec![Value::Int64(10)]];

        let path = format!("/tmp/db_{}.db", rand::random::<u64>());
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let mut table = HeapTable::new("t".into(), schema.clone(), 4, bp);
        table.insert_rows(rows);

        let scan = ScanExec::new(Arc::new(table), "t".into(), schema);
        let mut sort = SortExec::new(Box::new(scan), vec![(Expr::bound_col("t", "age"), true)]);

        sort.open();
        let first = sort.next().unwrap();

        assert_eq!(first.get("t.age"), Some(&Value::Int64(10)));
    }
}
