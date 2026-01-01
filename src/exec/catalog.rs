use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::storage::{btree::node::Index, table::Table};

pub struct IndexEntry {
    pub name: String,
    pub table: String,
    pub column: String,
    pub index: Arc<Mutex<dyn Index>>,
}

pub struct Catalog {
    pub tables: HashMap<String, Arc<dyn Table>>,
    pub indexes: HashMap<String, IndexEntry>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            indexes: HashMap::new(),
        }
    }

    pub fn add_index(
        &mut self,
        name: String,
        table: String,
        column: String,
        index: Arc<Mutex<dyn Index>>,
    ) {
        self.indexes.insert(
            name.clone(),
            IndexEntry {
                name,
                table,
                column,
                index,
            },
        );
    }

    pub fn drop_index(&mut self, name: &str) -> bool {
        self.indexes.remove(name).is_some()
    }

    pub fn get_index(&self, table: &str, column: &str) -> Option<Arc<Mutex<dyn Index>>> {
        self.indexes
            .values()
            .find(|e| e.table == table && e.column == column)
            .map(|e| e.index.clone())
    }

    pub fn insert(&mut self, name: String, table: Arc<dyn Table>) {
        self.tables.insert(name, table);
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Table>> {
        self.tables.get(name).cloned()
    }

    pub fn indexes_for_table(&self, table: &str) -> Vec<&IndexEntry> {
        self.indexes.values().filter(|e| e.table == table).collect()
    }
}
