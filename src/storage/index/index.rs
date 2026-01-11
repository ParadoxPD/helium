use crate::storage::{errors::StorageResult, index::btree::key::IndexKey, page::row_id::RowId};

pub trait Index: Send + Sync {
    fn insert(&mut self, key: IndexKey, rid: RowId) -> StorageResult<()>;
    fn delete(&mut self, key: &IndexKey, rid: RowId) -> StorageResult<()>;

    fn get(&self, key: &IndexKey) -> StorageResult<Vec<RowId>>;
    fn range(&self, low: &IndexKey, high: &IndexKey) -> StorageResult<Vec<RowId>>;
}
