use std::sync::Arc;

use crate::exec::operator::{Operator, Row};
use crate::storage::table::{Table, TableCursor};

pub struct ScanExec<'a> {
    table: Arc<dyn Table + 'a>,
    cursor: Option<Box<dyn TableCursor + 'a>>,
    alias: String,
}

impl<'a> ScanExec<'a> {
    pub fn new(table: Arc<dyn Table + 'a>, alias: String, _: Vec<String>) -> Self {
        Self {
            table,
            cursor: None,
            alias,
        }
    }
}

impl<'a> Operator for ScanExec<'a> {
    fn open(&mut self) {
        self.cursor = Some(self.table.clone().scan());
    }

    fn next(&mut self) -> Option<Row> {
        let storage_row = self.cursor.as_mut()?.next()?;
        let schema = self.table.schema();
        println!("SCHEMA  = {:?}", schema);

        let mut out = Row::new();
        for (col, value) in schema.iter().zip(storage_row.values.into_iter()) {
            out.insert(format!("{}.{}", self.alias, col), value);
        }

        debug_assert!(
            out.keys().all(|k| k.matches('.').count() == 1),
            "ScanExec must emit exactly one qualification level"
        );

        Some(out)
    }

    fn close(&mut self) {
        self.cursor = None;
    }
}

#[cfg(test)]
mod tests {
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
}
