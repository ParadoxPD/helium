use std::sync::{Arc, Mutex};

use crate::{
    common::value::Value,
    exec::operator::{Operator, Row},
    ir::plan::IndexPredicate,
    storage::{
        btree::node::{Index, IndexKey},
        page::{RowId, StorageRow},
        table::Table,
    },
};

pub struct IndexScanExec<'a> {
    table: Arc<dyn Table + 'a>,
    index: Arc<Mutex<dyn Index + 'a>>,
    predicate: IndexPredicate,

    table_alias: String,
    column: String,
    schema: Vec<String>,

    rids: Vec<RowId>,
    pos: usize,
}

impl<'a> IndexScanExec<'a> {
    pub fn new(
        table: Arc<dyn Table + 'a>,
        table_alias: String,
        index: Arc<Mutex<dyn Index + 'a>>,
        predicate: IndexPredicate,
        column: String,
        schema: Vec<String>,
    ) -> Self {
        Self {
            table,
            index,
            predicate,
            table_alias,
            column,
            schema,
            rids: Vec::new(),
            pos: 0,
        }
    }

    fn predicate_matches(row: &StorageRow, column_idx: usize, pred: &IndexPredicate) -> bool {
        let value = &row.values[column_idx];

        match pred {
            IndexPredicate::Eq(v) => value == v,

            IndexPredicate::Range { low, high } => match (value, low, high) {
                (Value::Int64(v), Value::Int64(l), Value::Int64(h)) => *v >= *l && *v <= *h,
                (Value::Float64(v), Value::Float64(l), Value::Float64(h)) => *v >= *l && *v <= *h,
                (Value::String(v), Value::String(l), Value::String(h)) => v >= l && v <= h,

                // NULL or mismatched types → predicate fails
                _ => false,
            },
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
                self.rids = self.index.lock().unwrap().get(&key.unwrap());
            }

            IndexPredicate::Range { low, high } => {
                let low_k = IndexKey::try_from(low);
                let high_k = IndexKey::try_from(high);
                self.rids = self
                    .index
                    .lock()
                    .unwrap()
                    .range(&low_k.unwrap(), &high_k.unwrap());
            }
        }
    }

    fn next(&mut self) -> Option<Row> {
        loop {
            if self.pos >= self.rids.len() {
                return None;
            }

            let rid = self.rids[self.pos];
            self.pos += 1;

            let storage = self.table.fetch(rid);

            let col_idx = self
                .table
                .schema()
                .iter()
                .position(|c| c == &self.column)
                .unwrap();

            if Self::predicate_matches(&storage, col_idx, &self.predicate) {
                let mut row = Row::new();

                if self.schema.len() == 1 && self.schema[0] == "*" {
                    // SELECT *
                    for (i, col) in self.table.schema().iter().enumerate() {
                        row.insert(
                            format!("{}.{}", self.table_alias, col),
                            storage.values[i].clone(),
                        );
                    }
                } else {
                    // explicit projection
                    for (i, col) in self.schema.iter().enumerate() {
                        row.insert(
                            format!("{}.{}", self.table_alias, col),
                            storage.values[i].clone(),
                        );
                    }
                }
                println!("ROWS in Index Scan = {:?}", row);

                return Some(row);
            }
        }
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
    use crate::storage::page_manager::FilePageManager;
    use crate::storage::table::{HeapTable, Table};
    use std::sync::{Arc, Mutex};

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
        let index: Arc<Mutex<dyn Index>> = Arc::new(Mutex::new(index));

        // -------- run index scan --------
        let mut exec = IndexScanExec::new(
            table.clone(),
            "users".into(),
            index.clone(),
            IndexPredicate::Eq(Value::Int64(20)),
            "age".into(),
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
            assert_eq!(r["users.age"], Value::Int64(20));
        }
    }
}
