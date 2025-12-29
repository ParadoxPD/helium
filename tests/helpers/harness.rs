use helium::api::db::{Database, QueryResult};
use helium::exec::operator::Row;

pub struct TestDB {
    db: Database,
}

#[allow(dead_code)]
impl TestDB {
    pub fn new() -> Self {
        Self {
            db: Database::new(),
        }
    }

    pub fn register_table(&mut self, name: &str, schema: Vec<String>, rows: Vec<Row>) {
        self.db.insert_table(name, schema, rows);
    }

    pub fn query(&self, sql: &str) -> Vec<Row> {
        match self.db.query(sql) {
            QueryResult::Rows(rows) => rows,
            other => panic!("Expected rows, got {:?}", other),
        }
    }

    pub fn explain(&self, sql: &str) -> String {
        match self.db.query(&format!("EXPLAIN {sql}")) {
            QueryResult::Explain(s) => s,
            other => panic!("Expected EXPLAIN output, got {:?}", other),
        }
    }

    pub fn explain_analyze(&self, sql: &str) -> String {
        match self.db.query(&format!("EXPLAIN ANALYZE {sql}")) {
            QueryResult::Explain(s) => s,
            other => panic!("Expected EXPLAIN ANALYZE output, got {:?}", other),
        }
    }
}
