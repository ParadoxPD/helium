use std::collections::HashMap;

use crate::{
    catalog::{catalog::Catalog, ids::TableId},
    execution::errors::ExecutionStats,
    storage::{buffer::pool::BufferPoolHandle, errors::StorageResult, heap::heap_table::HeapTable},
};
use std::sync::Arc;

pub struct ExecutionContext<'a> {
    pub catalog: &'a Catalog,
    pub buffer_pool: &'a BufferPoolHandle,
    pub stats: ExecutionStats,
    heap_tables: HashMap<TableId, Arc<HeapTable>>,
}

impl<'a> ExecutionContext<'a> {
    pub fn new(catalog: &'a Catalog, buffer_pool: &'a BufferPoolHandle) -> Self {
        Self {
            catalog,
            buffer_pool,
            stats: ExecutionStats {
                rows_output: 0,
                rows_scanned: 0,
                rows_filtered: 0,
                index_lookups: 0,
                storage_ops: 0,
            },
            heap_tables: HashMap::new(),
        }
    }

    pub fn get_heap(&mut self, table_id: TableId) -> StorageResult<Arc<HeapTable>> {
        if !self.heap_tables.contains_key(&table_id) {
            let heap = HeapTable::open(table_id, self.buffer_pool.clone())?;
            self.heap_tables.insert(table_id, Arc::new(heap));
        }
        Ok(self.heap_tables.get(&table_id).unwrap().clone())
    }
}

