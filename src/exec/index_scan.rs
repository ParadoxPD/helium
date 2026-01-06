use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    common::{
        schema::Schema,
        value::Value,
    },
    exec::operator::{Operator, Row},
    ir::plan::IndexPredicate,
    storage::{
        btree::node::{Index, IndexKey},
        page::{RowId, StorageRow},
        table::HeapTable,
    },
};

pub struct IndexScanExec<'a> {
    table: Arc<HeapTable>,
    index: Arc<Mutex<dyn Index + 'a>>,
    predicate: IndexPredicate,

    table_alias: String,
    column_name: String,
    schema: Schema,

    rids: Vec<RowId>,
    pos: usize,
}

impl<'a> IndexScanExec<'a> {
    pub fn new(
        table: Arc<HeapTable>,
        table_alias: String,
        index: Arc<Mutex<dyn Index + 'a>>,
        predicate: IndexPredicate,
        column_name: String,
        schema: Schema,
    ) -> Self {
        Self {
            table,
            index,
            predicate,
            table_alias,
            column_name,
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
                .columns
                .iter()
                .position(|c| c.name == self.column_name)
                .unwrap();

            if Self::predicate_matches(&storage, col_idx, &self.predicate) {
                let mut row = HashMap::new();

                if self.schema.columns.len() == 1 && self.schema.columns[0].name == "*" {
                    // SELECT *
                    for (i, col) in self.table.schema().columns.iter().enumerate() {
                        row.insert(
                            format!("{}.{}", self.table_alias, col.name),
                            storage.values[i].clone(),
                        );
                    }
                } else {
                    // explicit projection
                    for (i, col) in self.schema.columns.iter().enumerate() {
                        row.insert(
                            format!("{}.{}", self.table_alias, col.name),
                            storage.values[i].clone(),
                        );
                    }
                }
                println!("ROWS in Index Scan = {:?}", row);

                return Some(Row {
                    row_id: rid,
                    values: row,
                });
            }
        }
    }

    fn close(&mut self) {
        self.rids.clear();
        self.pos = 0;
    }
}
