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
    use std::sync::{Arc, Mutex};

    use crate::buffer::buffer_pool::BufferPool;
    use crate::common::value::Value;
    use crate::exec::limit::LimitExec;
    use crate::exec::operator::Operator;
    use crate::exec::scan::ScanExec;
    use crate::storage::page::{PageId, RowId, StorageRow};
    use crate::storage::page_manager::FilePageManager;
    use crate::storage::table::{HeapTable, Table};

    fn rows(vals: &[i64]) -> Vec<Vec<Value>> {
        vals.iter()
            .enumerate()
            .map(|(_, v)| vec![Value::Int64(*v)])
            .collect()
    }

    #[test]
    fn limit_returns_only_n_rows() {
        let schema = vec!["x".into()];
        let path = format!("/tmp/db_{}.db", rand::random::<u64>());

        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        // -------- create table --------
        let mut table = HeapTable::new("users".into(), schema.clone(), 4, bp.clone());

        table.insert_rows(rows(&[1, 2, 3]));

        let scan = ScanExec::new(Arc::new(table), "t".into(), schema);
        let mut limit = LimitExec::new(Box::new(scan), 2);

        limit.open();

        assert!(limit.next().is_some());
        assert!(limit.next().is_some());
        assert!(limit.next().is_none());
    }

    #[test]
    fn limit_zero_returns_no_rows() {
        let schema = vec!["x".into()];
        let path = format!("/tmp/db_{}.db", rand::random::<u64>());

        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        // -------- create table --------
        let mut table = HeapTable::new("users".into(), schema.clone(), 4, bp.clone());

        table.insert_rows(rows(&[1]));

        let scan = ScanExec::new(Arc::new(table), "t".into(), schema);
        let mut limit = LimitExec::new(Box::new(scan), 0);

        limit.open();
        assert!(limit.next().is_none());
    }

    #[test]
    fn limit_does_not_consume_extra_rows() {
        let schema = vec!["x".into()];
        let path = format!("/tmp/db_{}.db", rand::random::<u64>());

        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        // -------- create table --------
        let mut table = HeapTable::new("users".into(), schema.clone(), 4, bp.clone());

        table.insert(vec![Value::Int64(1)]);
        let scan = ScanExec::new(Arc::new(table), "t".into(), schema);
        let mut limit = LimitExec::new(Box::new(scan), 1);

        limit.open();
        let first = limit.next().unwrap();
        assert_eq!(first.get("t.x"), Some(&Value::Int64(1)));

        assert!(limit.next().is_none());
    }

    #[test]
    fn limit_resets_on_open() {
        let schema = vec!["x".into()];
        let path = format!("/tmp/db_{}.db", rand::random::<u64>());

        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        // -------- create table --------
        let mut table = HeapTable::new("users".into(), schema.clone(), 4, bp.clone());

        table.insert_rows(rows(&[1, 2]));

        let scan = ScanExec::new(Arc::new(table), "t".into(), schema);
        let mut limit = LimitExec::new(Box::new(scan), 1);

        limit.open();
        assert!(limit.next().is_some());
        assert!(limit.next().is_none());

        // reopen
        limit.open();
        assert!(limit.next().is_some());
    }
}
