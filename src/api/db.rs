use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::buffer::buffer_pool::{BufferPool, BufferPoolHandle};
use crate::common::schema::Schema;
use crate::common::value::Value;
use crate::exec::catalog::Catalog;
use crate::exec::expr_eval::{eval_predicate, eval_value};
use crate::exec::operator::Row;
use crate::exec::{execute_delete, execute_plan, execute_update};
use crate::frontend::sql::ast::ParseError;
use crate::frontend::sql::binder::{BindError, Binder, BoundStatement};
use crate::frontend::sql::lower::{Lowered, lower_select};
use crate::frontend::sql::parser::parse;
use crate::frontend::sql::{lower as sql_lower, parser, pretty_ast::pretty_ast};
use crate::ir::expr::Expr;
use crate::ir::pretty::pretty;
use crate::optimizer::optimize;
use crate::storage::btree::DiskBPlusTree;
use crate::storage::btree::node::IndexKey;
use crate::storage::page::RowId;
use crate::storage::page_manager::FilePageManager;

use anyhow::Result;

#[derive(Debug)]
pub enum QueryResult {
    Ok,
    Rows(Vec<Row>),
    Explain(String),
}

#[derive(Debug)]
pub enum QueryError {
    Parse(ParseError),
    Bind(BindError),
    Exec(anyhow::Error),
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::Parse(e) => write!(f, "Parse error: {:?}", e),
            QueryError::Bind(e) => write!(f, "Bind error: {:?}", e),
            QueryError::Exec(e) => write!(f, "Execution error: {}", e),
        }
    }
}

impl std::error::Error for QueryError {}

impl From<ParseError> for QueryError {
    fn from(e: ParseError) -> Self {
        QueryError::Parse(e)
    }
}

impl From<BindError> for QueryError {
    fn from(e: BindError) -> Self {
        QueryError::Bind(e)
    }
}

impl From<anyhow::Error> for QueryError {
    fn from(e: anyhow::Error) -> Self {
        QueryError::Exec(e)
    }
}

pub struct Database {
    pub catalog: Catalog,
    pub buffer_pool: BufferPoolHandle,
    pub path: PathBuf,
}

impl Database {
    pub fn new(path: String) -> Self {
        Self::open(PathBuf::from(path)).ok().unwrap()
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();

        let buffer_pool = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path)?,
        ))));

        Ok(Self {
            path,
            buffer_pool,
            catalog: Catalog::new(),
        })
    }

    pub fn create_index(
        &mut self,
        name: &str,
        table: &str,
        column: &str,
    ) -> Result<(), QueryError> {
        let table_ref = self
            .catalog
            .get_table(table)
            .ok_or_else(|| QueryError::Exec(anyhow::anyhow!("table '{}' not found", table)))?;

        let mut index = DiskBPlusTree::new(4, self.buffer_pool.clone());

        let col_idx = table_ref
            .schema
            .columns
            .iter()
            .position(|c| c.name == column)
            .ok_or_else(|| QueryError::Exec(anyhow::anyhow!("column '{}' not found", column)))?;

        let mut cursor = table_ref.heap.clone().scan();
        while let Some((rid, storage_row)) = cursor.next() {
            let key = IndexKey::try_from(&storage_row.values[col_idx]).map_err(|e| {
                QueryError::Exec(anyhow::anyhow!("failed to create index key: {:?}", e))
            })?;
            index.insert(key, rid);
        }

        let index = Arc::new(Mutex::new(index));

        self.catalog.add_index(
            name.to_string(),
            table.to_string(),
            column.to_string(),
            index,
        );

        Ok(())
    }

    pub fn create_table(&mut self, table_name: &str, schema: Schema) -> Result<(), QueryError> {
        self.catalog
            .create_table(table_name.into(), schema, self.buffer_pool.clone())
            .map_err(|e| QueryError::Exec(anyhow::anyhow!("failed to create table: {}", e)))
    }

    pub fn insert_values(&mut self, table: &str, rows: Vec<Vec<Value>>) -> Result<(), QueryError> {
        let table_ref = self
            .catalog
            .get_table(table)
            .ok_or_else(|| QueryError::Exec(anyhow::anyhow!("table '{}' not found", table)))?;

        for values in rows {
            let rid = table_ref.heap.insert(values.clone());

            // update indexes
            for entry in self.catalog.indexes_for_table(table) {
                let col_idx = table_ref
                    .schema
                    .columns
                    .iter()
                    .position(|c| c.name == entry.column)
                    .ok_or_else(|| {
                        QueryError::Exec(anyhow::anyhow!("column '{}' not found", entry.column))
                    })?;

                let key = IndexKey::try_from(&values[col_idx]).map_err(|e| {
                    QueryError::Exec(anyhow::anyhow!("failed to create index key: {:?}", e))
                })?;
                entry.index.lock().unwrap().insert(key, rid);
            }
        }

        Ok(())
    }

    pub fn delete_row(&mut self, table: &str, rid: RowId) -> Result<(), QueryError> {
        let table_ref = self
            .catalog
            .get_table(table)
            .ok_or_else(|| QueryError::Exec(anyhow::anyhow!("table '{}' not found", table)))?;

        let storage_row = table_ref.heap.fetch(rid);

        for idx in self.catalog.indexes_for_table(table) {
            let col_idx = table_ref
                .schema
                .columns
                .iter()
                .position(|c| c.name == idx.column)
                .ok_or_else(|| {
                    QueryError::Exec(anyhow::anyhow!("column '{}' not found", idx.column))
                })?;

            let key = IndexKey::try_from(&storage_row.values[col_idx]).map_err(|e| {
                QueryError::Exec(anyhow::anyhow!("failed to create index key: {:?}", e))
            })?;
            idx.index.lock().unwrap().delete(&key, rid);
        }

        table_ref.heap.delete(rid);
        Ok(())
    }

    pub fn update_row(
        &mut self,
        table: &str,
        rid: RowId,
        new_values: Vec<Value>,
    ) -> Result<(), QueryError> {
        let table_ref = self
            .catalog
            .get_table(table)
            .ok_or_else(|| QueryError::Exec(anyhow::anyhow!("table '{}' not found", table)))?;

        let old_row = table_ref.heap.fetch(rid);

        // remove old index entries
        for idx in self.catalog.indexes_for_table(table) {
            let col_idx = table_ref
                .schema
                .columns
                .iter()
                .position(|c| c.name == idx.column)
                .ok_or_else(|| {
                    QueryError::Exec(anyhow::anyhow!("column '{}' not found", idx.column))
                })?;

            let old_key = IndexKey::try_from(&old_row.values[col_idx]).map_err(|e| {
                QueryError::Exec(anyhow::anyhow!("failed to create index key: {:?}", e))
            })?;
            idx.index.lock().unwrap().delete(&old_key, rid);
        }

        // delete old row
        table_ref.heap.delete(rid);

        // insert new row
        let new_rid = table_ref.heap.insert(new_values.clone());

        // insert new index entries
        for idx in self.catalog.indexes_for_table(table) {
            let col_idx = table_ref
                .schema
                .columns
                .iter()
                .position(|c| c.name == idx.column)
                .ok_or_else(|| {
                    QueryError::Exec(anyhow::anyhow!("column '{}' not found", idx.column))
                })?;

            let new_key = IndexKey::try_from(&new_values[col_idx]).map_err(|e| {
                QueryError::Exec(anyhow::anyhow!("failed to create index key: {:?}", e))
            })?;
            idx.index.lock().unwrap().insert(new_key, new_rid);
        }

        Ok(())
    }

    pub fn debug_query(&mut self, sql: &str) -> Result<(), QueryError> {
        let stmt = parse(sql)?;
        println!("=== AST ===\n{}", pretty_ast(&stmt));

        let mut binder = Binder::new(&self.catalog);
        let bound = binder.bind_statement(stmt)?;
        println!("=== BOUND ===\n{:#?}", bound);

        if let BoundStatement::Select(s) = bound {
            let plan = lower_select(s);
            println!("=== LOGICAL PLAN ===\n{}", pretty(&plan));
        }

        Ok(())
    }

    pub fn run_query(&mut self, sql: &str) -> Result<QueryResult, QueryError> {
        // 1. Parse
        let stmt = parse(sql)?;

        // 2. Bind
        let mut binder = Binder::new(&self.catalog);
        let bound = binder.bind_statement(stmt)?;

        // 3. Execute
        match bound {
            BoundStatement::Select(s) => {
                let plan = lower_select(s);
                execute_plan(plan, &self.catalog).map_err(QueryError::Exec)
            }

            BoundStatement::CreateTable(ct) => {
                self.create_table(&ct.table, ct.schema)?;
                Ok(QueryResult::Ok)
            }

            BoundStatement::DropTable(dt) => {
                self.catalog
                    .drop_table(&dt.table)
                    .map_err(|e| QueryError::Exec(anyhow::anyhow!(e)))?;
                Ok(QueryResult::Ok)
            }

            BoundStatement::CreateIndex(ci) => {
                self.create_index(&ci.name, &ci.table, &ci.column)?;
                Ok(QueryResult::Ok)
            }

            BoundStatement::DropIndex(di) => {
                self.catalog.drop_index(&di.name);
                Ok(QueryResult::Ok)
            }

            BoundStatement::Insert(ins) => {
                let mut rows = Vec::new();

                for expr_row in ins.values {
                    let mut row = Vec::new();
                    match expr_row {
                        Expr::Literal(v) => row.push(v),
                        _ => {
                            return Err(QueryError::Exec(anyhow::anyhow!("non-literal in INSERT")));
                        }
                    }
                    rows.push(row);
                }

                self.insert_values(&ins.table, rows)?;

                Ok(QueryResult::Ok)
            }

            BoundStatement::Delete(del) => {
                execute_delete(del, &self.catalog)?;
                Ok(QueryResult::Ok)
            }

            BoundStatement::Update(upd) => {
                execute_update(upd, &self.catalog)?;
                Ok(QueryResult::Ok)
            }
        }
    }

    pub fn query(&mut self, sql: &str) -> Result<QueryResult, QueryError> {
        self.run_query(sql)
    }
}
