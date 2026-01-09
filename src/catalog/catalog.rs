use std::collections::HashMap;

use crate::catalog::errors::CatalogError;
use crate::catalog::ids::*;
use crate::catalog::index::{IndexEntry, IndexMeta};
use crate::catalog::table::TableMeta;

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
        columns: Vec<(String, crate::types::datatype::DataType, bool)>,
    ) -> Result<TableId, CatalogError> {
        if self.tables_by_name.contains_key(&name) {
            return Err(CatalogError::TableExists(name));
        }

        let table_id = TableId(self.next_table_id);
        self.next_table_id += 1;

        let mut cols = Vec::new();
        for (name, ty, nullable) in columns {
            let col_id = ColumnId(self.next_column_id);
            self.next_column_id += 1;

            cols.push(crate::catalog::column::ColumnMeta {
                id: col_id,
                name,
                data_type: ty,
                nullable,
            });
        }

        let meta = TableMeta {
            id: table_id,
            name: name.clone(),
            columns: cols,
        };

        self.tables_by_name.insert(name, table_id);
        self.tables_by_id.insert(table_id, meta);

        Ok(table_id)
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

        self.indexes_by_name.insert(name, index_id);
        self.indexes_by_id.insert(index_id, meta);

        Ok(index_id)
    }

    pub fn get_index_by_id(&self, id: IndexId) -> Option<&IndexMeta> {
        self.indexes_by_id.get(&id)
    }
}
