use crate::storage::page::row_id::RowId;
use crate::types::value::Value;

pub trait Index: Send + Sync {
    fn insert(&mut self, key: &Value, row_id: RowId);
    fn delete(&mut self, key: &Value, row_id: RowId);

    fn get(&self, key: &Value) -> Vec<RowId>;

    fn range(&self, low: &Value, high: &Value) -> Vec<RowId>;
}
