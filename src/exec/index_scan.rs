use std::sync::Arc;

use crate::{
    common::value::Value,
    exec::operator::{Operator, Row},
    ir::plan::IndexPredicate,
    storage::{
        btree::{
            DiskBPlusTree,
            node::{Index, IndexKey},
        },
        page::{RowId, StorageRow},
        table::Table,
    },
};

pub struct IndexScanExec<'a> {
    table: Arc<dyn Table + 'a>,
    index: Arc<dyn Index + 'a>,
    predicate: IndexPredicate,

    schema: Vec<String>,

    rids: Vec<RowId>,
    pos: usize,
}

impl<'a> IndexScanExec<'a> {
    pub fn new(
        table: Arc<dyn Table + 'a>,
        index: Arc<dyn Index + 'a>,
        predicate: IndexPredicate,
        schema: Vec<String>,
    ) -> Self {
        Self {
            table,
            index,
            predicate,
            schema,
            rids: Vec::new(),
            pos: 0,
        }
    }
}

impl<'a> Operator for IndexScanExec<'a> {
    fn open(&mut self) {
        self.rids.clear();
        self.pos = 0;

        match &self.predicate {
            IndexPredicate::Eq(v) => {
                let key = IndexKey::try_from(v);
                self.rids = self.index.get(&key.unwrap());
            }

            IndexPredicate::Range { low, high } => {
                let low_k = IndexKey::try_from(low);
                let high_k = IndexKey::try_from(high);
                self.rids = self.index.range(&low_k.unwrap(), &high_k.unwrap());
            }
        }
    }

    fn next(&mut self) -> Option<Row> {
        if self.pos >= self.rids.len() {
            return None;
        }

        let rid = self.rids[self.pos];
        self.pos += 1;

        Some(
            self.schema
                .iter()
                .cloned()
                .zip(self.table.fetch(rid).values.into_iter())
                .collect(),
        )
    }

    fn close(&mut self) {
        self.rids.clear();
        self.pos = 0;
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::buffer_pool::BufferPool;
    use crate::common::value::Value;
    use crate::exec::index_scan::IndexScanExec;
    use crate::exec::operator::Operator;
    use crate::ir::plan::IndexPredicate;
    use crate::storage::btree::DiskBPlusTree;
    use crate::storage::btree::node::{Index, IndexKey};
    use crate::storage::in_memory::InMemoryTable;
    use crate::storage::page::{PageId, RowId, StorageRow};
    use crate::storage::page_manager::FilePageManager;
    use crate::storage::table::{HeapTable, Table};
    use std::sync::{Arc, Mutex};

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
        let path = format!("/tmp/db_{}.db", rand::random::<u64>());

        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        // -------- create table --------
        let mut table = HeapTable::new(
            "users".into(),
            vec!["id".into(), "age".into()],
            4,
            bp.clone(),
        );

        // -------- create index --------
        let mut index = DiskBPlusTree::new(4, bp.clone());

        // -------- insert rows + index entries --------
        // We want age = 20 to appear twice
        let rows = vec![
            vec![Value::Int64(1), Value::Int64(20)],
            vec![Value::Int64(2), Value::Int64(20)],
            vec![Value::Int64(3), Value::Int64(30)],
            vec![Value::Int64(4), Value::Int64(40)],
        ];

        for row in rows {
            let rid = table.insert(row.clone());

            // index on "age" column (index key = row[1])
            let age = match &row[1] {
                Value::Int64(v) => *v,
                _ => unreachable!(),
            };

            index.insert(IndexKey::Int64(age), rid);
        }

        let table: Arc<dyn Table> = Arc::new(table);
        let index: Arc<dyn Index> = Arc::new(index);

        // -------- run index scan --------
        let mut exec = IndexScanExec::new(
            table.clone(),
            index.clone(),
            IndexPredicate::Eq(Value::Int64(20)),
            table.schema().to_vec(),
        );

        exec.open();

        let mut rows = Vec::new();
        while let Some(row) = exec.next() {
            rows.push(row);
        }

        // -------- assertions --------
        assert_eq!(rows.len(), 2);

        for r in rows {
            assert_eq!(r["age"], Value::Int64(20));
        }
    }
}
