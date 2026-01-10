use crate::storage::{
    errors::StorageResult,
    index::{
        btree::{BTreeIndex, key::IndexKey},
        index::Index,
    },
    page::row_id::RowId,
};

impl Index for BTreeIndex {
    fn insert(&mut self, key: IndexKey, row_id: RowId) -> StorageResult<()> {
        self.tree.insert(key, row_id)
    }

    fn delete(&mut self, key: &IndexKey, row_id: RowId) -> StorageResult<()> {
        self.tree.delete(key, row_id)
    }

    fn get(&self, key: &IndexKey) -> StorageResult<Vec<RowId>> {
        self.tree.get(key)
    }

    fn range(&self, low: &IndexKey, high: &IndexKey) -> StorageResult<Vec<RowId>> {
        self.tree.range(low, high)
    }
}
