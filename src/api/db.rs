use std::collections::HashMap;

use crate::common::value::Value;
use crate::exec::operator::Row;
use crate::exec::{Catalog, lower};
use crate::frontend::sql::lower::Lowered;
use crate::frontend::sql::{lower as sql_lower, parser};
use crate::ir::pretty::pretty;
use crate::optimizer::optimize;

pub struct Database {
    catalog: Catalog,
}

pub enum QueryResult {
    Rows(Vec<Row>),
    Explain(String),
}

impl Database {
    pub fn new() -> Self {
        Self {
            catalog: HashMap::new(),
        }
    }

    pub fn insert_table(&mut self, name: &str, rows: Vec<Row>) {
        self.catalog.insert(name.to_string(), rows);
    }

    pub fn query(&self, sql: &str) -> QueryResult {
        let stmt = parser::parse(sql);
        let lowered = sql_lower::lower_stmt(stmt);

        match lowered {
            Lowered::Plan(plan) => {
                let plan = optimize(&plan);
                let mut exec = lower(&plan, &self.catalog);
                exec.open();

                let mut rows = Vec::new();
                while let Some(row) = exec.next() {
                    rows.push(row);
                }
                QueryResult::Rows(rows)
            }

            Lowered::Explain { analyze, plan } => {
                let plan = optimize(&plan);

                if !analyze {
                    QueryResult::Explain(pretty(&plan));
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

        db.insert_table(
            "users",
            vec![
                row(&[
                    ("t.name", Value::String("Alice".into())),
                    ("t.age", Value::Int64(30)),
                    ("t.score", Value::Int64(80)),
                ]),
                row(&[
                    ("t.name", Value::String("Bob".into())),
                    ("t.age", Value::Int64(15)),
                    ("t.score", Value::Int64(90)),
                ]),
                row(&[
                    ("t.name", Value::String("Carol".into())),
                    ("t.age", Value::Int64(40)),
                    ("t.score", Value::Int64(40)),
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

        db.insert_table(
            "users",
            vec![
                row(&[
                    ("t.name", Value::String("Bob".into())),
                    ("t.age", Value::Int64(30)),
                ]),
                row(&[
                    ("t.name", Value::String("Alice".into())),
                    ("t.age", Value::Int64(20)),
                ]),
            ],
        );

        match db.query("SELECT name FROM users ORDER BY age ASC;") {
            QueryResult::Rows(rows) => {
                println!("ROWS = {:#?}", rows);
                assert_eq!(rows[0].get("name"), Some(&Value::String("Alice".into())));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn sql_join_works() {
        let mut db = Database::new();

        db.insert_table(
            "users",
            vec![row(&[
                ("u.id", Value::Int64(1)),
                ("u.name", Value::String("Alice".into())),
            ])],
        );

        db.insert_table(
            "orders",
            vec![row(&[
                ("o.user_id", Value::Int64(1)),
                ("o.amount", Value::Int64(200)),
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
