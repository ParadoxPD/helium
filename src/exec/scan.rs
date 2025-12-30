use std::sync::Arc;

use crate::exec::operator::{Operator, Row};
use crate::storage::page::StorageRow;
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
    use std::sync::Arc;

    use crate::{
        common::value::Value,
        exec::{operator::Operator, scan::ScanExec},
        storage::{
            in_memory::InMemoryTable,
            page::{PageId, RowId, StorageRow},
            table::Table,
        },
    };

    fn srow(slot: u16, values: Vec<Value>) -> StorageRow {
        StorageRow {
            rid: RowId {
                page_id: PageId(0),
                slot_id: slot,
            },
            values,
        }
    }

    #[test]
    fn scan_returns_all_rows() {
        let schema = vec!["id".into()];
        let rows = vec![
            srow(0, vec![Value::Int64(1)]),
            srow(1, vec![Value::Int64(2)]),
        ];

        let table = Arc::new(InMemoryTable::new("t".into(), schema.clone(), rows));

        let mut scan = ScanExec::new(table, "t".into(), schema);
        scan.open();

        assert!(scan.next().is_some());
        assert!(scan.next().is_some());
        assert!(scan.next().is_none());
    }

    #[test]
    fn table_cursor_emits_distinct_row_ids() {
        let schema = vec!["id".into()];
        let rows = vec![
            srow(0, vec![Value::Int64(1)]),
            srow(1, vec![Value::Int64(2)]),
        ];

        let table = Arc::new(InMemoryTable::new("users".into(), schema, rows));

        let mut cursor = table.scan();

        let r1 = cursor.next().unwrap();
        let r2 = cursor.next().unwrap();

        assert_ne!(r1.rid, r2.rid);
    }
}
