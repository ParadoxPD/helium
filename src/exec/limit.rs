use crate::{
    exec::operator::{Operator, Row},
    storage::page::StorageRow,
};

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

    use crate::common::value::Value;
    use crate::exec::limit::LimitExec;
    use crate::exec::operator::Operator;
    use crate::exec::scan::ScanExec;
    use crate::storage::in_memory::InMemoryTable;
    use crate::storage::page::{PageId, RowId, StorageRow};

    fn rows(vals: &[i64]) -> Vec<StorageRow> {
        vals.iter()
            .enumerate()
            .map(|(i, v)| StorageRow {
                rid: RowId {
                    page_id: PageId(0),
                    slot_id: i as u16,
                },
                values: vec![Value::Int64(*v)],
            })
            .collect()
    }

    #[test]
    fn limit_returns_only_n_rows() {
        let schema = vec!["x".into()];
        let table = Arc::new(InMemoryTable::new(
            "t".into(),
            schema.clone(),
            rows(&[1, 2, 3]),
        ));

        let scan = ScanExec::new(table, "t".into(), schema);
        let mut limit = LimitExec::new(Box::new(scan), 2);

        limit.open();

        assert!(limit.next().is_some());
        assert!(limit.next().is_some());
        assert!(limit.next().is_none());
    }

    #[test]
    fn limit_zero_returns_no_rows() {
        let schema = vec!["x".into()];
        let table = Arc::new(InMemoryTable::new("t".into(), schema.clone(), rows(&[1])));

        let scan = ScanExec::new(table, "t".into(), schema);
        let mut limit = LimitExec::new(Box::new(scan), 0);

        limit.open();
        assert!(limit.next().is_none());
    }

    #[test]
    fn limit_does_not_consume_extra_rows() {
        let schema = vec!["x".into()];
        let table = Arc::new(InMemoryTable::new(
            "t".into(),
            schema.clone(),
            rows(&[1, 2]),
        ));

        let scan = ScanExec::new(table, "t".into(), schema);
        let mut limit = LimitExec::new(Box::new(scan), 1);

        limit.open();
        let first = limit.next().unwrap();
        assert_eq!(first.get("t.x"), Some(&Value::Int64(1)));

        assert!(limit.next().is_none());
    }

    #[test]
    fn limit_resets_on_open() {
        let schema = vec!["x".into()];
        let table = Arc::new(InMemoryTable::new(
            "t".into(),
            schema.clone(),
            rows(&[1, 2]),
        ));

        let scan = ScanExec::new(table, "t".into(), schema);
        let mut limit = LimitExec::new(Box::new(scan), 1);

        limit.open();
        assert!(limit.next().is_some());
        assert!(limit.next().is_none());

        // reopen
        limit.open();
        assert!(limit.next().is_some());
    }
}
