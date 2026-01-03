use std::collections::HashMap;
use std::sync::Arc;

use crate::{
    exec::operator::{Operator, Row},
    storage::table::{HeapTable, TableCursor},
};

pub struct ScanExec<'a> {
    table: Arc<HeapTable>,
    cursor: Option<Box<dyn TableCursor + 'a>>,
    alias: String,
}

impl<'a> ScanExec<'a> {
    pub fn new(table: Arc<HeapTable>, alias: String, _: Vec<String>) -> Self {
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
        let (rid, storage_row) = self.cursor.as_mut()?.next()?;
        let schema = self.table.schema();

        let mut out = HashMap::with_capacity(schema.columns.len());

        for (col, value) in schema.columns.iter().zip(storage_row.values.iter()) {
            out.insert(format!("{}.{}", self.alias, col.name), value.clone());
        }

        debug_assert!(
            out.keys().all(|k| k.matches('.').count() == 1),
            "ScanExec must emit exactly one qualification level"
        );

        Some(Row {
            row_id: rid,
            values: out,
        })
    }

    fn close(&mut self) {
        self.cursor = None;
    }
}
