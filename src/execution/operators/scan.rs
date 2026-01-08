use std::collections::HashMap;
use std::sync::Arc;

use crate::{
    db_info, db_trace,
    debugger::Component,
    exec::{
        evaluator::ExecError,
        operator::{Operator, Row},
    },
    storage::table::{HeapTable, TableCursor},
};

pub struct ScanExec<'a> {
    table: Arc<HeapTable>,
    cursor: Option<Box<dyn TableCursor + 'a>>,
    alias: String,
}

impl<'a> ScanExec<'a> {
    pub fn new(table: Arc<HeapTable>, alias: String, _: Vec<String>) -> Self {
        println!("SCANNING {}", alias);
        Self {
            table,
            cursor: None,
            alias,
        }
    }
}

impl<'a> Operator for ScanExec<'a> {
    fn open(&mut self) -> Result<(), ExecError> {
        db_info!(
            Component::Executor,
            "Opening scan on table '{}'",
            self.alias
        );
        self.cursor = Some(self.table.clone().scan());
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Row>, ExecError> {
        db_trace!(Component::Executor, "ScanExec::next() on '{}'", self.alias);
        let cursor = match self.cursor.as_mut() {
            Some(c) => c,
            None => return Ok(None), // not opened or already closed
        };

        let (rid, storage_row) = match cursor.next() {
            Some(v) => v,
            None => return Ok(None), // end of scan
        };
        db_trace!(
            Component::Executor,
            "Found row: rid={:?}, values={:?}",
            rid,
            storage_row.values
        );

        let schema = self.table.schema();

        let mut out = HashMap::with_capacity(schema.columns.len());

        for (col, value) in schema.columns.iter().zip(storage_row.values.iter()) {
            out.insert(format!("{}.{}", self.alias, col.name), value.clone());
        }

        debug_assert!(
            out.keys().all(|k| k.matches('.').count() == 1),
            "ScanExec must emit exactly one qualification level"
        );

        Ok(Some(Row {
            row_id: rid,
            values: out,
        }))
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.cursor = None;
        Ok(())
    }
}
