use crate::storage::{
    index::{
        btree::{BTreeIndex, key::IndexKey},
        index::Index,
    },
    page::row_id::RowId,
};

impl Index for BTreeIndex {
    fn insert(&mut self, key: IndexKey, row_id: RowId) {
        self.tree.insert(key, row_id);
    }

    fn delete(&mut self, key: &IndexKey, row_id: RowId) {
        self.tree.delete(key, row_id);
    }

    fn get(&self, key: &IndexKey) -> Vec<RowId> {
        self.tree.get(key)
    }

    fn range(&self, low: &IndexKey, high: &IndexKey) -> Vec<RowId> {
        self.tree.range(low, high)
    }
}
