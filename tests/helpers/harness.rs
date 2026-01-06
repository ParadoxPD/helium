use helium::api::db::{Database, QueryError, QueryResult};

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
    pub fn exec(&mut self, sql: &str) -> Result<QueryResult, QueryError> {
        self.db.run_query(sql)
    }

    /// Execute a SELECT and return rows.
    #[allow(dead_code)]
    pub fn query(&mut self, sql: &str) -> Result<Vec<helium::exec::operator::Row>, anyhow::Error> {
        match self.exec(sql)? {
            QueryResult::Rows(rows) => Ok(rows),
            other => anyhow::bail!("Expected rows, got {:?}", other),
        }
    }

    /// EXPLAIN without ANALYZE
    #[allow(dead_code)]
    pub fn explain(&mut self, sql: &str) -> Result<String, anyhow::Error> {
        match self.exec(&format!("EXPLAIN {sql}"))? {
            QueryResult::Explain(s) => Ok(s),
            other => anyhow::bail!("Expected EXPLAIN, got {:?}", other),
        }
    }

    /// EXPLAIN ANALYZE
    #[allow(dead_code)]
    pub fn explain_analyze(&mut self, sql: &str) -> Result<String, anyhow::Error> {
        match self.exec(&format!("EXPLAIN ANALYZE {sql}"))? {
            QueryResult::Explain(s) => Ok(s),
            other => anyhow::bail!("Expected EXPLAIN ANALYZE, got {:?}", other),
        }
    }
}
