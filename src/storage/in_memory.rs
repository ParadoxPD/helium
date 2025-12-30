use std::collections::HashMap;
use std::sync::Arc;

use crate::common::schema::Schema;
use crate::exec::operator::Row;
use crate::storage::btree::BPlusTree;
use crate::storage::btree::node::{Index, IndexKey};
use crate::storage::page::{RowId, StorageRow};
use crate::storage::table::{Table, TableCursor};

pub struct InMemoryTable {
    table_name: String,
    schema: Schema,
    rows: Vec<StorageRow>,
    indexes: HashMap<String, BPlusTree>,
}

impl InMemoryTable {
    pub fn new(table_name: String, schema: Schema, rows: Vec<StorageRow>) -> Self {
        Self {
            table_name,
            schema,
            rows,
            indexes: HashMap::new(),
        }
    }

    pub fn create_index(&mut self, column: &str, order: usize) {
        let col_idx = self
            .schema
            .iter()
            .position(|c| c == column)
            .expect("column not found");
        let mut index = BPlusTree::new(order);

        for row in &self.rows {
            if let Ok(key) = IndexKey::try_from(&row.values[col_idx]) {
                index.insert(key, row.rid);
            }
        }

        self.indexes.insert(column.to_string(), index);
    }

    pub fn insert(&mut self, row: StorageRow) {
        let rid = row.rid;

        for (i, col) in self.schema.iter().enumerate() {
            if let Some(index) = self.indexes.get_mut(col) {
                if let Ok(key) = IndexKey::try_from(&row.values[i]) {
                    index.insert(key, rid);
                }
            }
        }

        self.rows.push(row);
    }

    pub fn update(&mut self, rid: RowId, new_row: StorageRow) -> bool {
        let old = match self.rows.get(rid.slot_id as usize) {
            Some(r) => r.clone(),
            None => return false,
        };

        for (i, col) in self.schema.iter().enumerate() {
            if let Some(index) = self.indexes.get_mut(col) {
                if let Ok(old_key) = IndexKey::try_from(&old.values[i]) {
                    index.delete(&old_key, rid);
                }
                if let Ok(new_key) = IndexKey::try_from(&new_row.values[i]) {
                    index.insert(new_key, rid);
                }
            }
        }

        self.rows[rid.slot_id as usize] = new_row;
        true
    }
}

impl Table for InMemoryTable {
    fn scan(self: Arc<Self>) -> Box<dyn TableCursor> {
        Box::new(InMemoryCursor::new(self))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn fetch(&self, rid: RowId) -> StorageRow {
        let row = &self.rows[rid.slot_id as usize];
        debug_assert_eq!(row.rid, rid);
        row.clone()
    }

    fn get_index(&self, column: &str) -> Option<&dyn Index> {
        self.indexes.get(column).map(|i| i as &dyn Index)
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
