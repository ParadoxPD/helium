use crate::exec::operator::{Operator, Row};

pub struct LimitExec {
    input: Box<dyn Operator>,
    limit: usize,
    seen: usize,
}

impl LimitExec {
    pub fn new(input: Box<dyn Operator>, limit: usize) -> Self {
        Self {
            input,
            limit,
            seen: 0,
        }
    }
}

impl Operator for LimitExec {
    fn open(&mut self) {
        self.seen = 0;
        self.input.open();
    }

    fn next(&mut self) -> Option<Row> {
        if self.seen >= self.limit {
            return None;
        }

        match self.input.next() {
            Some(row) => {
                self.seen += 1;
                Some(row)
            }
            None => None,
        }
    }

    fn close(&mut self) {
        self.input.close();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::common::value::Value;
    use crate::exec::operator::Operator;
    use crate::exec::scan::ScanExec;
    use crate::exec::test_util::qrow;
    use crate::storage::in_memory::InMemoryTable;

    #[test]
    fn limit_returns_only_n_rows() {
        let data = vec![
            qrow("t", &[("x", Value::Int64(1))]),
            qrow("t", &[("x", Value::Int64(2))]),
            qrow("t", &[("x", Value::Int64(3))]),
        ];

        let scan = ScanExec::new(Arc::new(InMemoryTable::new("t".into(), data)));
        let mut limit = LimitExec::new(Box::new(scan), 2);

        limit.open();

        assert!(limit.next().is_some());
        assert!(limit.next().is_some());
        assert!(limit.next().is_none());
    }

    #[test]
    fn limit_zero_returns_no_rows() {
        let data = vec![qrow("t", &[("x", Value::Int64(1))])];

        let scan = ScanExec::new(Arc::new(InMemoryTable::new("t".into(), data)));
        let mut limit = LimitExec::new(Box::new(scan), 0);

        limit.open();
        assert!(limit.next().is_none());
    }

    #[test]
    fn limit_does_not_consume_extra_rows() {
        let data = vec![
            qrow("t", &[("x", Value::Int64(1))]),
            qrow("t", &[("x", Value::Int64(2))]),
        ];

        let scan = ScanExec::new(Arc::new(InMemoryTable::new("t".into(), data)));
        let mut limit = LimitExec::new(Box::new(scan), 1);

        limit.open();
        let first = limit.next().unwrap();
        assert_eq!(first.get("t.x"), Some(&Value::Int64(1)));

        // Should not pull the second row
        assert!(limit.next().is_none());
    }

    #[test]
    fn limit_resets_on_open() {
        let data = vec![
            qrow("t", &[("x", Value::Int64(1))]),
            qrow("t", &[("x", Value::Int64(2))]),
        ];

        let scan = ScanExec::new(Arc::new(InMemoryTable::new("t".into(), data)));
        let mut limit = LimitExec::new(Box::new(scan), 1);

        limit.open();
        assert!(limit.next().is_some());
        assert!(limit.next().is_none());

        // reopen
        limit.open();
        assert!(limit.next().is_some());
    }
}
