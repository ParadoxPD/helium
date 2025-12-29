use std::sync::Arc;

use crate::exec::operator::{Operator, Row};
use crate::storage::table::{Table, TableCursor};

pub struct ScanExec {
    table: Arc<dyn Table>,
    cursor: Option<Box<dyn TableCursor>>,
}

impl ScanExec {
    pub fn new(table: Arc<dyn Table>) -> Self {
        Self {
            table,
            cursor: None,
        }
    }
}

impl Operator for ScanExec {
    fn open(&mut self) {
        self.cursor = Some(self.table.scan());
    }

    fn next(&mut self) -> Option<Row> {
        let row = self.cursor.as_mut()?.next();

        if let Some(ref r) = row {
            eprintln!("[ScanExec] row = {:?}", r);
        } else {
            eprintln!("[ScanExec] EOF");
        }

        debug_assert!(
            row.clone()?.keys().all(|k| k.contains('.')),
            "ScanExec must output base-qualified columns"
        );

        Some(row.unwrap())
    }

    fn close(&mut self) {
        self.cursor = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        common::value::Value, exec::operator::Operator, storage::in_memory::InMemoryTable,
    };

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

        let mut scan = ScanExec::new(Arc::new(InMemoryTable::new("t".into(), data)));
        scan.open();

        assert!(scan.next().is_some());
        assert!(scan.next().is_some());
        assert!(scan.next().is_none());
    }
}
