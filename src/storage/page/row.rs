use crate::types::value::Value;

#[derive(Debug, Clone)]
pub struct StorageRow {
    pub values: Vec<Value>,
}

impl StorageRow {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }
}
