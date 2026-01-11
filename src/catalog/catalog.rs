use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::catalog::column::ColumnMeta;
use crate::catalog::errors::CatalogError;
use crate::catalog::ids::*;
use crate::catalog::index::{IndexEntry, IndexMeta};
use crate::catalog::table::TableMeta;
use crate::storage::buffer::pool::{BufferPool, BufferPoolHandle};
use crate::storage::index::btree::BTreeIndex;
use crate::storage::index::btree::disk::BPlusTree;
use crate::storage::index::index::Index;
use crate::storage::pagemgr::file::FilePageManager;
use crate::types::datatype::DataType;
use crate::types::schema::Schema;

#[derive(Debug, Clone)]
pub struct TableStats {
    pub row_count: u64,
}

pub struct Catalog {
    next_table_id: u32,
    next_column_id: u32,
    next_index_id: u32,

    tables_by_id: HashMap<TableId, TableMeta>,
    tables_by_name: HashMap<String, TableId>,

    indexes_by_id: HashMap<IndexId, IndexEntry>,
    indexes_by_name: HashMap<String, IndexId>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            next_table_id: 1,
            next_column_id: 1,
            next_index_id: 1,
            tables_by_id: HashMap::new(),
            tables_by_name: HashMap::new(),
            indexes_by_id: HashMap::new(),
            indexes_by_name: HashMap::new(),
        }
    }

    // ---------- table API ----------

    pub fn create_table(
        &mut self,
        name: String,
        columns: Vec<(String, DataType, bool)>,
    ) -> Result<TableId, CatalogError> {
        if self.tables_by_name.contains_key(&name) {
            return Err(CatalogError::TableExists(name));
        }

        let table_id = TableId(self.next_table_id);
        self.next_table_id += 1;

        let mut schema = Schema::new();
        for (name, ty, nullable) in columns {
            let col_id = ColumnId(self.next_column_id);
            self.next_column_id += 1;

            schema.push(ColumnMeta {
                id: col_id,
                name,
                data_type: ty,
                nullable,
            });
        }

        let meta = TableMeta {
            id: table_id,
            name: name.clone(),
            schema,
            root_page: None,       // ADD THIS
            index_ids: Vec::new(), // ADD THIS
        };

        self.tables_by_name.insert(name, table_id);
        self.tables_by_id.insert(table_id, meta);

        Ok(table_id)
    }

    // Add method to register index with table
    pub fn register_index_with_table(&mut self, table_id: TableId, index_id: IndexId) {
        if let Some(table) = self.tables_by_id.get_mut(&table_id) {
            table.index_ids.push(index_id);
        }
    }
    pub fn get_table_by_id(&self, id: TableId) -> Option<&TableMeta> {
        self.tables_by_id.get(&id)
    }

    pub fn get_table_by_name(&self, name: &str) -> Option<&TableMeta> {
        self.tables_by_name
            .get(name)
            .and_then(|id| self.tables_by_id.get(id))
    }

    // ---------- index API ----------

    pub fn create_index(
        &mut self,
        name: String,
        table_id: TableId,
        column_ids: Vec<ColumnId>,
        unique: bool,
        bp: BufferPoolHandle,
    ) -> Result<IndexId, CatalogError> {
        if self.indexes_by_name.contains_key(&name) {
            return Err(CatalogError::IndexExists(name));
        }

        let index_id = IndexId(self.next_index_id);
        self.next_index_id += 1;

        let meta = IndexMeta {
            id: index_id,
            name: name.clone(),
            table_id,
            column_ids,
            unique,
        };

        // Create the actual B+Tree index
        let tree = BPlusTree::new(100, bp)?; // order = 100
        let index = Arc::new(Mutex::new(BTreeIndex::new(tree)));

        let entry = IndexEntry { meta, index };

        self.indexes_by_name.insert(name, index_id);
        self.indexes_by_id.insert(index_id, entry);

        Ok(index_id)
    }

    // Add this method for getting index by name
    pub fn get_index_by_name(&self, name: &str) -> Option<&IndexEntry> {
        self.indexes_by_name
            .get(name)
            .and_then(|id| self.indexes_by_id.get(id))
    }

    // Add this method for table statistics (needed by optimizer)
    pub fn table_stats(&self, table_id: TableId) -> TableStats {
        // For now, return dummy stats. In Phase 2, this will be persisted
        TableStats {
            row_count: 1000, // placeholder
        }
    }
    pub fn get_index_by_id(&self, id: IndexId) -> Option<&IndexEntry> {
        self.indexes_by_id.get(&id)
    }

    pub fn indexes_for_table(&self, table_id: TableId) -> impl Iterator<Item = &IndexEntry> {
        self.indexes_by_id
            .values()
            .filter(move |i| i.meta.table_id == table_id)
    }

    pub fn find_index_on_column(
        &self,
        table_id: TableId,
        column_id: ColumnId,
    ) -> Option<&IndexEntry> {
        self.indexes_by_id.values().find(|idx| {
            idx.meta.table_id == table_id
                && idx.meta.column_ids.len() == 1
                && idx.meta.column_ids[0] == column_id
        })
    }
}
