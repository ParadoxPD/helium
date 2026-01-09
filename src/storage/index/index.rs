use crate::storage::{index::btree::key::IndexKey, page::row_id::RowId};

pub trait Index: Send + Sync {
    fn insert(&mut self, key: IndexKey, rid: RowId);
    fn delete(&mut self, key: &IndexKey, rid: RowId);

    fn get(&self, key: &IndexKey) -> Vec<RowId>;
    fn range(&self, low: &IndexKey, high: &IndexKey) -> Vec<RowId>;
}
