use crate::types::value::Value;

#[derive(Clone, Debug)]
pub struct StorageRow {
    pub values: Vec<Value>,
}
