use std::collections::HashMap;

use crate::common::value::Value;
use crate::exec::operator::Row;
use crate::exec::{Catalog, lower};
use crate::frontend::sql::{lower as sql_lower, parser};
use crate::optimizer::optimize;

pub struct Database {
    catalog: Catalog,
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

    pub fn query(&self, sql: &str) -> Vec<Row> {
        let ast = parser::parse(sql);
        let plan = sql_lower::lower(ast);
        let plan = optimize(&plan);

        let mut exec = lower(&plan, &self.catalog);
        exec.open();

        let mut results = Vec::new();
        while let Some(row) = exec.next() {
            results.push(row);
        }

        results
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

        let results = db.query("SELECT name FROM users WHERE age > 18 AND score > 50 LIMIT 1;");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("name"), Some(&Value::String("Alice".into())));
    }
}
