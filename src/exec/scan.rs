use crate::common::value::Value;
use crate::exec::operator::{Operator, Row};

pub struct ScanExec {
    data: Vec<Row>,
    cursor: usize,
}

impl ScanExec {
    pub fn new(data: Vec<Row>) -> Self {
        Self { data, cursor: 0 }
    }
}

impl Operator for ScanExec {
    fn open(&mut self) {
        self.cursor = 0;
    }

    fn next(&mut self) -> Option<Row> {
        if self.cursor >= self.data.len() {
            return None;
        }

        let row = self.data[self.cursor].clone();
        self.cursor += 1;
        debug_assert!(
            row.keys().all(|k| k.contains('.')),
            "ScanExec produced unqualified row: {:?}",
            row
        );

        Some(row)
    }

    fn close(&mut self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::operator::Operator;

    #[test]
    fn scan_returns_all_rows() {
        let data = vec![
            [("t.id", Value::Int64(1))]
                .into_iter()
                .map(|(k, v)| (k.into(), v))
                .collect(),
            [("t.id", Value::Int64(2))]
                .into_iter()
                .map(|(k, v)| (k.into(), v))
                .collect(),
        ];

        let mut scan = ScanExec::new(data);
        scan.open();

        assert!(scan.next().is_some());
        assert!(scan.next().is_some());
        assert!(scan.next().is_none());
    }
}
