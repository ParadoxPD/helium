use crate::common::value::Value;
use std::collections::HashMap;

pub type Row = HashMap<String, Value>;

pub trait Operator {
    fn open(&mut self);
    fn next(&mut self) -> Option<Row>;
    fn close(&mut self);
}

#[derive(Default, Debug)]
pub struct ExecStats {
    pub rows: usize,
    pub elapsed_ns: u128,
}
