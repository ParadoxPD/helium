use std::sync::Arc;

use crate::common::schema::Schema;
use crate::exec::operator::Row;
use crate::storage::page::StorageRow;
use crate::storage::table::{Table, TableCursor};

pub struct InMemoryTable {
    table_name: String,
    schema: Schema,
    rows: Vec<StorageRow>,
}

impl InMemoryTable {
    pub fn new(table_name: String, schema: Schema, rows: Vec<StorageRow>) -> Self {
        Self {
            table_name,
            schema,
            rows,
        }
    }
}

impl Table for InMemoryTable {
    fn scan(self: Arc<Self>) -> Box<dyn TableCursor> {
        Box::new(InMemoryCursor::new(self))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

struct InMemoryCursor {
    table: String,
    rows: Vec<StorageRow>,
    pos: usize,
}

impl InMemoryCursor {
    pub fn new(table: Arc<InMemoryTable>) -> Self {
        Self {
            table: table.table_name.clone(),
            rows: table.rows.clone(),
            pos: 0,
        }
    }
}

impl TableCursor for InMemoryCursor {
    fn next(&mut self) -> Option<StorageRow> {
        if self.pos >= self.rows.len() {
            return None;
        }
        let row = &self.rows[self.pos];

        self.pos += 1;
        Some(row.clone())
    }
}
