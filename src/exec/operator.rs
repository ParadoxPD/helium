use crate::{common::value::Value, exec::evaluator::ExecError, storage::page::RowId};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub row_id: RowId,
    pub values: HashMap<String, Value>,
}
impl Default for Row {
    fn default() -> Self {
        Self {
            row_id: RowId::default(),
            values: HashMap::new(),
        }
    }
}

pub trait Operator {
    fn open(&mut self) -> Result<(), ExecError>;
    fn next(&mut self) -> Result<Option<Row>, ExecError>;
    fn close(&mut self) -> Result<(), ExecError>;
}

#[derive(Default, Debug)]
pub struct ExecStats {
    pub rows: usize,
    pub elapsed_ns: u128,
}
