use crate::{
    catalog::catalog::Catalog, execution::errors::ExecutionStats,
    storage::buffer::pool::BufferPoolHandle,
};

pub struct ExecutionContext<'a> {
    pub catalog: &'a Catalog,
    pub buffer_pool: &'a BufferPoolHandle,

    /// Shared execution statistics
    pub stats: ExecutionStats,
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
        }
    }
}
