use crate::common::value::Value;
use std::collections::HashMap;
use std::time::Instant;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_can_store_values() {
        let mut row = Row::new();
        row.insert("age".into(), Value::Int64(30));

        assert_eq!(row.get("age"), Some(&Value::Int64(30)));
    }
}
