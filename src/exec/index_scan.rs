use std::sync::Arc;

use crate::{
    common::value::Value,
    exec::operator::{Operator, Row},
    storage::{btree::node::IndexKey, page::RowId, table::Table},
};

pub struct IndexScanExec<'a> {
    table: Arc<dyn Table + 'a>,
    index_col: String,
    key: Value,

    alias: String,
    schema: Vec<String>,

    rids: Vec<RowId>,
    pos: usize,
}

impl<'a> IndexScanExec<'a> {
    pub fn new(
        table: Arc<dyn Table + 'a>,
        index_col: String,
        key: Value,
        alias: String,
        schema: Vec<String>,
    ) -> Self {
        Self {
            table,
            index_col,
            key,
            alias,
            schema,
            rids: Vec::new(),
            pos: 0,
        }
    }
}

impl<'a> Operator for IndexScanExec<'a> {
    fn open(&mut self) {
        let index = self
            .table
            .get_index(&self.index_col)
            .expect("IndexScanExec: index not found");

        let key = IndexKey::try_from(&self.key).expect("IndexScanExec: invalid index key type");

        self.rids = index.get(&key);
        self.pos = 0;
    }

    fn next(&mut self) -> Option<Row> {
        if self.pos >= self.rids.len() {
            return None;
        }

        let rid = self.rids[self.pos];
        self.pos += 1;

        let storage_row = self.table.fetch(rid);

        let mut out = Row::new();
        let schema = self.table.schema();

        for (col, value) in schema.iter().zip(storage_row.values.into_iter()) {
            out.insert(format!("{}.{}", self.alias, col), value);
        }

        debug_assert!(
            out.keys().all(|k| k.matches('.').count() == 1),
            "IndexScanExec must emit exactly one qualification level"
        );

        Some(out)
    }

    fn close(&mut self) {
        self.rids.clear();
        self.pos = 0;
    }
}

#[cfg(test)]
mod tests {
    use crate::common::value::Value;
    use crate::exec::index_scan::IndexScanExec;
    use crate::exec::operator::Operator;
    use crate::storage::in_memory::InMemoryTable;
    use crate::storage::page::{PageId, RowId, StorageRow};
    use std::sync::Arc;

    fn make_table() -> Arc<InMemoryTable> {
        let mut table =
            InMemoryTable::new("users".into(), vec!["id".into(), "age".into()], Vec::new());

        table.insert(StorageRow {
            rid: RowId {
                page_id: PageId(0),
                slot_id: 0,
            },
            values: vec![Value::Int64(1), Value::Int64(20)],
        });

        table.insert(StorageRow {
            rid: RowId {
                page_id: PageId(0),
                slot_id: 1,
            },
            values: vec![Value::Int64(2), Value::Int64(30)],
        });

        table.insert(StorageRow {
            rid: RowId {
                page_id: PageId(0),
                slot_id: 2,
            },
            values: vec![Value::Int64(3), Value::Int64(20)],
        });

        table.create_index("age", 4);
        Arc::new(table)
    }

    #[test]
    fn index_scan_exec_returns_matching_rows() {
        let table = make_table();

        let mut exec = IndexScanExec::new(
            table.clone(),
            "age".into(),
            Value::Int64(20),
            "users".into(),
            vec!["id".into(), "age".into()],
        );

        exec.open();

        let mut rows = Vec::new();
        while let Some(row) = exec.next() {
            rows.push(row);
        }

        exec.close();

        assert_eq!(rows.len(), 2);

        for r in rows {
            assert_eq!(r["users.age"], Value::Int64(20));
        }
    }
}
