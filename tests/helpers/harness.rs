use helium::api::db::{Database, QueryResult};
use helium::exec::operator::Row;

use helium::frontend::sql::{binder::Binder, parser::parse};

pub struct TestDB {
    pub db: Database,
}

impl TestDB {
    pub fn new() -> Self {
        let path = format!("/tmp/helium_test_{}.db", rand::random::<u64>());
        let db = Database::open(path).unwrap();
        Self { db }
    }

    /// Execute any SQL statement.
    /// This is the *only* entry point tests should use.
    pub fn exec(&mut self, sql: &str) -> Result<QueryResult, anyhow::Error> {
        self.db
            .run_query(sql)
            .map_err(|e| anyhow::anyhow!("{:?}", e))
    }

    /// Execute a SELECT and return rows.
    pub fn query(&mut self, sql: &str) -> Result<Vec<helium::exec::operator::Row>, anyhow::Error> {
        match self.exec(sql)? {
            QueryResult::Rows(rows) => Ok(rows),
            other => anyhow::bail!("Expected rows, got {:?}", other),
        }
    }

    /// EXPLAIN without ANALYZE
    pub fn explain(&mut self, sql: &str) -> Result<String, anyhow::Error> {
        match self.exec(&format!("EXPLAIN {sql}"))? {
            QueryResult::Explain(s) => Ok(s),
            other => anyhow::bail!("Expected EXPLAIN, got {:?}", other),
        }
    }

    /// EXPLAIN ANALYZE
    pub fn explain_analyze(&mut self, sql: &str) -> Result<String, anyhow::Error> {
        match self.exec(&format!("EXPLAIN ANALYZE {sql}"))? {
            QueryResult::Explain(s) => Ok(s),
            other => anyhow::bail!("Expected EXPLAIN ANALYZE, got {:?}", other),
        }
    }
}
