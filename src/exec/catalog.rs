use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::{Result, anyhow, bail};

use crate::{
    buffer::buffer_pool::{BufferPool, BufferPoolHandle},
    common::{schema::Schema, value::Value},
    storage::{btree::node::Index, page::RowId, page_manager::FilePageManager, table::HeapTable},
};

const DEFAULT_PAGE_CAPACITY: usize = 256;

#[derive(Clone)]
pub struct IndexEntry {
    pub name: String,
    pub table: String,
    pub column: String,
    pub index: Arc<Mutex<dyn Index>>,
}

#[derive(Clone)]
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

    pub fn create_table(
        &mut self,
        table_name: String,
        schema: Schema,
        buffer_pool: BufferPoolHandle,
    ) -> Result<()> {
        println!("Creating table in catalog : {}", table_name);
        if self.tables.contains_key(&table_name) {
            bail!("table already exists");
        }

        let heap = Arc::new(HeapTable::new(
            table_name.clone(),
            schema.clone(),
            DEFAULT_PAGE_CAPACITY,
            buffer_pool,
        ));

        self.tables.insert(
            table_name.clone(),
            TableEntry {
                name: table_name,
                schema,
                heap,
            },
        );
        println!("{:?}", self.tables.keys());

        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        self.tables
            .remove(name)
            .ok_or_else(|| anyhow!("table not found"))?;

        self.indexes.retain(|_, e| e.table != name);

        Ok(())
    }

    pub fn insert_row(&self, table: &str, values: Vec<Value>) -> Result<RowId> {
        let entry = self
            .tables
            .get(table)
            .ok_or_else(|| anyhow!("table not found"))?;

        Ok(entry.heap.insert(values))
    }
}
