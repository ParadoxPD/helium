use std::sync::{Arc, Mutex};

use crate::{
    common::value::Value,
    exec::operator::{Operator, Row},
    frontend::sql::binder::Column,
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
    column: Column,
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
        column: Column,
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
                            format!("{}.{}", self.table_alias, col.name),
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
