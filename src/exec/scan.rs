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
            out.insert(format!("{}.{}", self.alias, col.name), value);
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
