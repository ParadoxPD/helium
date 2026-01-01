use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::{Result, anyhow, bail};

use crate::{
    buffer::buffer_pool::BufferPool,
    common::schema::Schema,
    storage::{
        btree::node::Index,
        page_manager::FilePageManager,
        table::{HeapTable, Table},
    },
};

const DEFAULT_PAGE_CAPACITY: usize = 2048;

pub struct IndexEntry {
    pub name: String,
    pub table: String,
    pub column: String,
    pub index: Arc<Mutex<dyn Index>>,
}

pub struct Catalog {
    pub tables: HashMap<String, TableEntry>,
    pub indexes: HashMap<String, IndexEntry>,
}

#[derive(Clone)]
pub struct TableEntry {
    pub name: String,
    pub schema: Schema,
    pub heap: Arc<HeapTable>,
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

    pub fn get_table(&self, name: &str) -> Option<&TableEntry> {
        self.tables.get(name)
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut TableEntry> {
        self.tables.get_mut(name)
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    pub fn indexes_for_table(&self, table: &str) -> Vec<&IndexEntry> {
        self.indexes.values().filter(|e| e.table == table).collect()
    }

    pub fn create_table(&mut self, name: String, schema: Schema) -> Result<()> {
        if self.tables.contains_key(&name) {
            bail!("table already exists");
        }

        let path = format!("/tmp/{}.db", name);

        let buffer_pool = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let heap = Arc::new(HeapTable::new(
            name.clone(),
            schema.clone(),
            DEFAULT_PAGE_CAPACITY,
            buffer_pool.clone(),
        ));

        self.tables
            .insert(name.clone(), TableEntry { name, schema, heap });

        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        self.tables
            .remove(name)
            .ok_or_else(|| anyhow!("table not found"))?;

        self.indexes.retain(|_, e| e.table != name);

        Ok(())
    }
}
