use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::{
    catalog::{catalog::Catalog, ids::IndexId, ids::TableId},
    storage::{
        buffer::pool::BufferPoolHandle, errors::StorageResult, heap::heap_table::HeapTable,
        index::index::Index,
    },
};

pub struct StorageManager {
    catalog: Arc<Catalog>,
    buffer_pool: BufferPoolHandle,
    heaps: HashMap<TableId, Arc<HeapTable>>,
    indexes: HashMap<IndexId, Arc<Mutex<dyn Index>>>,
}

impl StorageManager {
    pub fn new(catalog: Arc<Catalog>, buffer_pool: BufferPoolHandle) -> Self {
        Self {
            catalog,
            buffer_pool,
            heaps: HashMap::new(),
            indexes: HashMap::new(),
        }
    }

    pub fn get_table(&mut self, id: TableId) -> StorageResult<Arc<HeapTable>> {
        if !self.heaps.contains_key(&id) {
            let heap = HeapTable::open(id, self.buffer_pool.clone())?;
            self.heaps.insert(id, Arc::new(heap));
        }
        Ok(self.heaps.get(&id).unwrap().clone())
    }

    pub fn get_index(&mut self, id: IndexId) -> StorageResult<Arc<Mutex<dyn Index>>> {
        if !self.indexes.contains_key(&id) {
            // Load index from catalog
            if let Some(idx_entry) = self.catalog.get_index_by_id(id) {
                self.indexes.insert(id, idx_entry.index.clone());
            }
        }
        Ok(self.indexes.get(&id).unwrap().clone())
    }
}

