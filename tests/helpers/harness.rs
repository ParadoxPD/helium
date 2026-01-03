use helium::api::db::{Database, QueryResult};
use helium::exec::operator::Row;

use helium::{
    exec::statement::execute_statement,
    frontend::sql::{binder::Binder, parser::parse},
};

pub struct TestDB {
    pub db: Database,
}

impl TestDB {
    pub fn new() -> Self {
        let path = format!("/tmp/helium_test_{}.db", rand::random::<u64>());
        let db = Database::open(path).unwrap();
        Self { db }
    }

    pub fn exec(&mut self, sql: &str) -> Result<QueryResult, anyhow::Error> {
        let stmt = parse(sql);
        let binder = Binder::new(&self.db.catalog);
        Ok(execute_statement(
            stmt,
            &mut self.db.catalog,
            &binder,
            self.db.buffer_pool.clone(),
        )?)
    }

    pub fn query(&mut self, sql: &str) -> Result<Vec<helium::exec::operator::Row>, anyhow::Error> {
        match self.exec(sql)? {
            QueryResult::Rows(rows) => Ok(rows),
            other => anyhow::bail!("Expected rows, got {:?}", other),
        }
    }

    pub fn explain(&mut self, sql: &str) -> String {
        match self.exec(&format!("EXPLAIN {sql}")).unwrap() {
            QueryResult::Explain(s) => s,
            other => panic!("Expected EXPLAIN, got {:?}", other),
        }
    }

    pub fn explain_analyze(&mut self, sql: &str) -> String {
        match self.exec(&format!("EXPLAIN ANALYZE {sql}")).unwrap() {
            QueryResult::Explain(s) => s,
            other => panic!("Expected EXPLAIN ANALYZE, got {:?}", other),
        }
    }
}
