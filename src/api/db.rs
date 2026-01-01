use std::sync::{Arc, Mutex};

use crate::buffer::buffer_pool::{BufferPool, BufferPoolHandle};
use crate::common::schema::Schema;
use crate::common::value::Value;
use crate::exec::catalog::Catalog;
use crate::exec::lower;
use crate::exec::operator::Row;
use crate::frontend::sql::lower::Lowered;
use crate::frontend::sql::{lower as sql_lower, parser, pretty_ast::pretty_ast};
use crate::ir::pretty::pretty;
use crate::optimizer::optimize;
use crate::storage::btree::DiskBPlusTree;
use crate::storage::btree::node::IndexKey;
use crate::storage::page::RowId;
use crate::storage::page_manager::FilePageManager;
use crate::storage::table::{HeapTable, Table};

#[derive(Debug)]
pub enum QueryResult {
    Rows(Vec<Row>),
    Explain(String),
}

pub struct Database {
    catalog: Catalog,
    bp: BufferPoolHandle,
}

impl Database {
    pub fn new() -> Self {
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open("/tmp/db.db").unwrap(),
        ))));
        Self {
            catalog: Catalog::new(),
            bp,
        }
    }

    pub fn create_index(&mut self, name: &str, table: &str, column: &str) {
        let table_ref = self.catalog.get_table(table).expect("table not found");

        // 1. Create disk B+Tree
        let mut index = DiskBPlusTree::new(4, self.bp.clone());

        // 2. Scan table and populate index
        let mut cursor = table_ref.heap.clone().scan();
        while let Some(row) = cursor.next() {
            let col_idx = table_ref
                .schema
                .iter()
                .position(|c| c.name == column)
                .expect("column not found");

            let key = IndexKey::try_from(&row.values[col_idx]).unwrap();
            index.insert(key, row.rid);
        }

        let index = Arc::new(Mutex::new(index));
        // 3. Register in catalog
        self.catalog.add_index(
            name.to_string(),
            table.to_string(),
            column.to_string(),
            index,
        );
    }

    pub fn insert_table(&mut self, table: &str, schema: Schema, rows: Vec<Row>) {
        let path = format!("/tmp/{}.db", table);

        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let mut heap = HeapTable::new(table.to_string(), schema.clone(), 128, bp.clone());

        for row in rows {
            let mut values = Vec::with_capacity(schema.len());

            for col in &schema {
                let v = row
                    .get(&format!("{table}.{col}"))
                    .or_else(|| row.get(col))
                    .cloned()
                    .unwrap_or(Value::Null);

                values.push(v);
            }

            let rid = heap.insert(values.clone());

            for entry in self.catalog.indexes_for_table(table) {
                let col_idx = heap
                    .schema()
                    .iter()
                    .position(|c| c.name == entry.column)
                    .unwrap();

                let key = IndexKey::try_from(&values[col_idx]).unwrap();
                entry.index.lock().unwrap().insert(key, rid);
            }
        }
        self.catalog.insert(table.to_string(), Arc::new(heap));
    }

    pub fn delete_row(&self, table: &str, rid: RowId) {
        let table_ref = self.catalog.get_table(table).unwrap();

        // fetch row BEFORE delete
        let row = table_ref.heap.fetch(rid);

        // index maintenance
        for idx in self.catalog.indexes_for_table(table) {
            let col_idx = table_ref
                .schema
                .iter()
                .position(|c| c.name == idx.column)
                .unwrap();

            let key = IndexKey::try_from(&row.values[col_idx]).unwrap();
            idx.index.lock().unwrap().delete(&key, rid);
        }

        // delete from heap
        table_ref.delete(rid);
    }

    pub fn update_row(&self, table: &str, rid: RowId, new_values: Vec<Value>) {
        let table_ref = self.catalog.get_table(table).unwrap();

        let old_row = table_ref.heap.fetch(rid);

        // remove old keys
        for idx in self.catalog.indexes_for_table(table) {
            let col_idx = table_ref
                .schema
                .iter()
                .position(|c| c.name == idx.column)
                .unwrap();
            let old_key = IndexKey::try_from(&old_row.values[col_idx]).unwrap();
            idx.index.lock().unwrap().delete(&old_key, rid);
        }

        // update heap
        table_ref.update(rid, new_values.clone());

        // insert new keys
        for idx in self.catalog.indexes_for_table(table) {
            let col_idx = table_ref
                .schema
                .iter()
                .position(|c| c.name == idx.column)
                .unwrap();
            let new_key = IndexKey::try_from(&new_values[col_idx]).unwrap();
            idx.index.lock().unwrap().insert(new_key, rid);
        }
    }

    pub fn debug_query(&mut self, sql: &str) -> Lowered {
        let stmt = parser::parse(sql);
        println!("=== AST ===\n{}", pretty_ast(&stmt));

        let lowered = sql_lower::lower_stmt(stmt, &self.catalog);

        match &lowered {
            Lowered::Plan(p) => {
                println!("=== LOGICAL PLAN ===\n{}", pretty(p));
            }
            Lowered::Explain { plan, .. } => {
                println!("=== EXPLAIN PLAN ===\n{}", pretty(plan));
            }
            Lowered::CreateIndex {
                name,
                table,
                column,
            } => {
                println!("======CREATE INDEX======\n{} {} {}", name, table, column);
            }
            Lowered::DropIndex { name } => {
                println!("====DROP INDEX======\n{}", name);
            }
        }
        lowered
    }

    pub fn query(&mut self, sql: &str) -> QueryResult {
        let lowered = self.debug_query(sql);
        match lowered {
            Lowered::Plan(plan) => {
                let plan = optimize(&plan, &self.catalog);
                let mut exec = lower(&plan, &self.catalog);
                exec.open();

                let mut rows = Vec::new();
                while let Some(row) = exec.next() {
                    rows.push(row);
                }

                QueryResult::Rows(rows)
            }

            Lowered::Explain { analyze, plan } => {
                let plan = optimize(&plan, &self.catalog);

                if !analyze {
                    return QueryResult::Explain(pretty(&plan));
                }

                let start = std::time::Instant::now();
                let mut exec = lower(&plan, &self.catalog);
                exec.open();

                let mut rows = 0;
                while exec.next().is_some() {
                    rows += 1;
                }

                let elapsed = start.elapsed().as_micros();
                QueryResult::Explain(format!(
                    "{}\n\nrows={} time={}µs",
                    pretty(&plan),
                    rows,
                    elapsed
                ))
            }
            Lowered::CreateIndex {
                name,
                table,
                column,
            } => {
                self.create_index(&name, &table, &column);
                QueryResult::Explain(format!("Index {} created", name))
            }
            Lowered::DropIndex { name } => {
                let dropped = self.catalog.drop_index(&name);
                if !dropped {
                    panic!("index not found");
                }
                QueryResult::Explain(format!("Index {} dropped", name))
            }
        }
    }
}
