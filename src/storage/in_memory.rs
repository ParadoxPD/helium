use crate::exec::operator::Row;
use crate::storage::table::{Table, TableCursor};

pub struct InMemoryTable {
    table_name: String,
    rows: Vec<Row>,
}

impl InMemoryTable {
    pub fn new(table_name: String, rows: Vec<Row>) -> Self {
        Self { table_name, rows }
    }
}

impl Table for InMemoryTable {
    fn scan(&self) -> Box<dyn TableCursor> {
        Box::new(InMemoryCursor {
            table: self.table_name.clone(),
            rows: self.rows.clone(),
            pos: 0,
        })
    }
}

struct InMemoryCursor {
    table: String,
    rows: Vec<Row>,
    pos: usize,
}

impl TableCursor for InMemoryCursor {
    fn next(&mut self) -> Option<Row> {
        if self.pos >= self.rows.len() {
            return None;
        }

        let mut out = Row::new();
        let row = &self.rows[self.pos];

        for (k, v) in row {
            out.insert(format!("{}.{}", self.table, k), v.clone());
        }

        self.pos += 1;
        Some(out)
    }
}
