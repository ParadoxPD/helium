use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::buffer::buffer_pool::BufferPool;
use crate::common::schema::Schema;
use crate::common::value::Value;
use crate::exec::operator::Row;
use crate::exec::{Catalog, lower};
use crate::frontend::sql::lower::Lowered;
use crate::frontend::sql::{lower as sql_lower, parser};
use crate::ir::pretty::pretty;
use crate::optimizer::optimize;
use crate::storage::in_memory::InMemoryTable;
use crate::storage::page::{PageId, RowId, StorageRow};
use crate::storage::page_manager::FilePageManager;
use crate::storage::table::{HeapTable, Table};

#[derive(Debug)]
pub enum QueryResult {
    Rows(Vec<Row>),
    Explain(String),
}

pub struct Database {
    catalog: Catalog,
}

impl Database {
    pub fn new() -> Self {
        Self {
            catalog: Catalog::new(),
        }
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

            heap.insert(values);
        }

        self.catalog.insert(table.to_string(), Arc::new(heap));
    }

    pub fn query(&self, sql: &str) -> QueryResult {
        let stmt = parser::parse(sql);
        let lowered = sql_lower::lower_stmt(stmt);

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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::value::Value;

    fn row(pairs: &[(&str, Value)]) -> Row {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn sql_query_with_and_and_limit() {
        let mut db = Database::new();

        let schema = vec!["name".into(), "age".into(), "score".into()];

        db.insert_table(
            "users",
            schema,
            vec![
                row(&[
                    ("name", Value::String("Alice".into())),
                    ("age", Value::Int64(30)),
                    ("score", Value::Int64(80)),
                ]),
                row(&[
                    ("name", Value::String("Bob".into())),
                    ("age", Value::Int64(15)),
                    ("score", Value::Int64(90)),
                ]),
                row(&[
                    ("name", Value::String("Carol".into())),
                    ("age", Value::Int64(40)),
                    ("score", Value::Int64(40)),
                ]),
            ],
        );

        match db.query("SELECT name FROM users WHERE age > 18 AND score > 50 LIMIT 1;") {
            QueryResult::Rows(results) => {
                assert_eq!(results.len(), 1);
                assert_eq!(results[0].get("name"), Some(&Value::String("Alice".into())));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn sql_order_by_works() {
        let mut db = Database::new();

        let schema = vec!["name".into(), "age".into()];

        db.insert_table(
            "users",
            schema,
            vec![
                row(&[
                    ("name", Value::String("Bob".into())),
                    ("age", Value::Int64(30)),
                ]),
                row(&[
                    ("name", Value::String("Alice".into())),
                    ("age", Value::Int64(20)),
                ]),
            ],
        );

        match db.query("SELECT name FROM users ORDER BY age ASC;") {
            QueryResult::Rows(rows) => {
                assert_eq!(rows[0].get("name"), Some(&Value::String("Alice".into())));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn sql_join_works() {
        let mut db = Database::new();

        let users_schema = vec!["id".into(), "name".into()];

        let orders_schema = vec!["user_id".into(), "amount".into()];

        db.insert_table(
            "users",
            users_schema,
            vec![row(&[
                ("id", Value::Int64(1)),
                ("name", Value::String("Alice".into())),
            ])],
        );

        db.insert_table(
            "orders",
            orders_schema,
            vec![row(&[
                ("user_id", Value::Int64(1)),
                ("amount", Value::Int64(200)),
            ])],
        );

        match db.query("SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id;") {
            QueryResult::Rows(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get("name"), Some(&Value::String("Alice".into())));
            }
            _ => panic!("expected rows"),
        }
    }
}
