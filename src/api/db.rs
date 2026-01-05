use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::buffer::buffer_pool::{BufferPool, BufferPoolHandle};
use crate::common::schema::Schema;
use crate::common::value::Value;
use crate::debugger::debugger::reset_indent;
use crate::debugger::{Component, DebugLevel, get_debug_level, get_report, reset};
use crate::exec::catalog::Catalog;
use crate::exec::operator::Row;
use crate::exec::{execute_delete, execute_plan, execute_update};
use crate::frontend::sql::ast::ParseError;
use crate::frontend::sql::binder::{BindError, Binder, BoundStatement};
use crate::frontend::sql::lower::lower_select;
use crate::frontend::sql::parser::Parser;
use crate::ir::expr::Expr;
use crate::ir::pretty::pretty;
use crate::storage::btree::DiskBPlusTree;
use crate::storage::btree::node::IndexKey;
use crate::storage::page::RowId;
use crate::storage::page_manager::FilePageManager;
use crate::{db_debug, db_error, db_info, db_phase, db_scope, db_trace, db_warn};

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
        println!("Creating table {}", table_name);

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

    pub fn run_query(&mut self, sql: &str) -> Result<QueryResult, QueryError> {
        use crate::debugger::debugger::{Component, DebugLevel, get_debug_level, reset_indent};
        use crate::debugger::phases::{get_report, reset};

        reset();
        reset_indent();

        db_info!(Component::Planner, "=== Starting query execution ===");
        db_info!(Component::Planner, "SQL: {}", sql);

        let result = db_phase!("Total Query Execution", {
            // 1. Parse
            db_scope!(DebugLevel::Debug, Component::Parser, "Parse Phase", {
                let stmts = db_phase!("Parse SQL", {
                    db_debug!(Component::Parser, "Parsing SQL: {}", sql);
                    let stmts = Parser::new(sql).parse_statements()?;
                    db_info!(Component::Parser, "Parsed {} statement(s)", stmts.len());
                    for (i, stmt) in stmts.iter().enumerate() {
                        db_debug!(Component::Parser, "Statement {}: {:?}", i + 1, stmt);
                    }
                    Ok::<_, QueryError>(stmts)
                })?;

                let mut last_result = None;

                for (stmt_idx, stmt) in stmts.into_iter().enumerate() {
                    db_info!(
                        Component::Planner,
                        "--- Processing statement {} ---",
                        stmt_idx + 1
                    );

                    // 2. Bind
                    let bound = db_phase!(format!("Bind Statement {}", stmt_idx + 1), {
                        db_debug!(Component::Binder, "Binding statement: {:?}", stmt);
                        let mut binder = Binder::new(&self.catalog);
                        let bound = binder.bind_statement(stmt)?;
                        db_info!(
                            Component::Binder,
                            "Bound statement type: {}",
                            match &bound {
                                BoundStatement::Select(_) => "SELECT",
                                BoundStatement::Insert(_) => "INSERT",
                                BoundStatement::Delete(_) => "DELETE",
                                BoundStatement::Update(_) => "UPDATE",
                                BoundStatement::CreateTable(_) => "CREATE TABLE",
                                BoundStatement::DropTable(_) => "DROP TABLE",
                                BoundStatement::CreateIndex(_) => "CREATE INDEX",
                                BoundStatement::DropIndex(_) => "DROP INDEX",
                                BoundStatement::Explain { .. } => "EXPLAIN",
                            }
                        );
                        db_debug!(Component::Binder, "Bound AST: {:?}", bound);
                        Ok::<_, QueryError>(bound)
                    })?;

                    // 3. Execute
                    last_result = Some(db_phase!(
                        format!("Execute Statement {}", stmt_idx + 1),
                        {
                            match bound {
                                BoundStatement::Select(s) => {
                                    db_scope!(
                                        DebugLevel::Debug,
                                        Component::Planner,
                                        "SELECT Execution",
                                        {
                                            // Lower to logical plan
                                            let plan = db_phase!("Lower to Logical Plan", {
                                                db_debug!(
                                                    Component::Planner,
                                                    "Lowering bound SELECT to logical plan"
                                                );
                                                let plan = lower_select(s);
                                                db_debug!(
                                                    Component::Planner,
                                                    "Logical plan:\n{}",
                                                    pretty(&plan)
                                                );
                                                plan
                                            });

                                            // Execute
                                            db_phase!("Execute Query", {
                                                db_debug!(
                                                    Component::Executor,
                                                    "Creating execution tree"
                                                );
                                                let mut exec = execute_plan(plan, &self.catalog);

                                                db_debug!(Component::Executor, "Opening operators");
                                                //exec.open();

                                                let mut rows: Vec<Row> = Vec::new();
                                                let mut row_count = 0;

                                                //db_debug!(Component::Executor, "Fetching rows");
                                                //while let Some(row) = exec.next() {
                                                //    row_count += 1;
                                                //    db_trace!(
                                                //        Component::Executor,
                                                //        "Row {}: {:?}",
                                                //        row_count,
                                                //        row
                                                //    );
                                                //    rows.push(row);
                                                //}

                                                db_info!(
                                                    Component::Executor,
                                                    "Query returned {} row(s)",
                                                    rows.len()
                                                );
                                                //exec.close();

                                                //Ok(QueryResult::Rows(rows))
                                                exec.map_err(QueryError::Exec)
                                            })
                                        }
                                    )
                                }

                                BoundStatement::Explain { analyze, stmt } => {
                                    db_scope!(
                                        DebugLevel::Debug,
                                        Component::Planner,
                                        "EXPLAIN Execution",
                                        {
                                            match *stmt {
                                                BoundStatement::Select(s) => {
                                                    let plan =
                                                        db_phase!("Lower to Logical Plan", {
                                                            let plan = lower_select(s);
                                                            db_debug!(
                                                                Component::Planner,
                                                                "Plan for EXPLAIN:\n{}",
                                                                pretty(&plan)
                                                            );
                                                            plan
                                                        });

                                                    //let optimized = db_phase!("Optimize Plan", {
                                                    //optimize(&plan, &self.catalog)
                                                    //});

                                                    if analyze {
                                                        db_info!(
                                                            Component::Executor,
                                                            "Running EXPLAIN ANALYZE"
                                                        );

                                                        let start = std::time::Instant::now();
                                                        //let mut exec =
                                                        //  lower_select(&optimized, &self.catalog);
                                                        //exec.open();

                                                        let mut row_count = 0;
                                                        //while let Some(_) = exec.next() {
                                                        //   row_count += 1;
                                                        //}
                                                        //exec.close();

                                                        let elapsed = start.elapsed();

                                                        db_info!(
                                                            Component::Executor,
                                                            "ANALYZE: {} rows in {:?}",
                                                            row_count,
                                                            elapsed
                                                        );

                                                        let output = format!(
                                                            "EXPLAIN ANALYZE\n\n\nExecution time: {:.2}ms\nRows: {}",
                                                            //pretty(&optimized),
                                                            elapsed.as_secs_f64() * 1000.0,
                                                            row_count
                                                        );

                                                        Ok(QueryResult::Explain(output))
                                                    } else {
                                                        db_info!(
                                                            Component::Planner,
                                                            "Returning EXPLAIN plan"
                                                        );
                                                        let output =
                                                            format!("EXPLAIN\n{}", pretty(&plan));
                                                        Ok(QueryResult::Explain(output))
                                                    }
                                                }

                                                _ => {
                                                    db_error!(
                                                        Component::Planner,
                                                        "EXPLAIN only supports SELECT statements"
                                                    );
                                                    Err(QueryError::Exec(anyhow::anyhow!(
                                                        "EXPLAIN only supported for SELECT"
                                                    )))
                                                }
                                            }
                                        }
                                    )
                                }

                                BoundStatement::CreateTable(ct) => {
                                    db_scope!(
                                        DebugLevel::Debug,
                                        Component::Storage,
                                        "CREATE TABLE",
                                        {
                                            db_info!(
                                                Component::Storage,
                                                "Creating table '{}'",
                                                ct.table
                                            );
                                            db_debug!(
                                                Component::Storage,
                                                "Schema: {:?}",
                                                ct.schema
                                            );

                                            self.create_table(&ct.table, ct.schema)?;

                                            db_info!(
                                                Component::Storage,
                                                "Table '{}' created successfully",
                                                ct.table
                                            );
                                            Ok(QueryResult::Ok)
                                        }
                                    )
                                }

                                BoundStatement::DropTable(dt) => {
                                    db_scope!(
                                        DebugLevel::Debug,
                                        Component::Storage,
                                        "DROP TABLE",
                                        {
                                            db_info!(
                                                Component::Storage,
                                                "Dropping table '{}'",
                                                dt.table
                                            );

                                            self.catalog.drop_table(&dt.table).map_err(|e| {
                                                db_error!(
                                                    Component::Storage,
                                                    "Failed to drop table: {}",
                                                    e
                                                );
                                                QueryError::Exec(anyhow::anyhow!(e))
                                            })?;

                                            db_info!(
                                                Component::Storage,
                                                "Table '{}' dropped successfully",
                                                dt.table
                                            );
                                            Ok(QueryResult::Ok)
                                        }
                                    )
                                }

                                BoundStatement::CreateIndex(ci) => {
                                    db_scope!(
                                        DebugLevel::Debug,
                                        Component::BTree,
                                        "CREATE INDEX",
                                        {
                                            db_info!(
                                                Component::BTree,
                                                "Creating index '{}' on {}.{}",
                                                ci.name,
                                                ci.table,
                                                ci.column
                                            );

                                            self.create_index(&ci.name, &ci.table, &ci.column)?;

                                            db_info!(
                                                Component::BTree,
                                                "Index '{}' created successfully",
                                                ci.name
                                            );
                                            Ok(QueryResult::Ok)
                                        }
                                    )
                                }

                                BoundStatement::DropIndex(di) => {
                                    db_scope!(DebugLevel::Debug, Component::BTree, "DROP INDEX", {
                                        db_info!(Component::BTree, "Dropping index '{}'", di.name);

                                        let dropped = self.catalog.drop_index(&di.name);

                                        if dropped {
                                            db_info!(
                                                Component::BTree,
                                                "Index '{}' dropped successfully",
                                                di.name
                                            );
                                        } else {
                                            db_warn!(
                                                Component::BTree,
                                                "Index '{}' not found",
                                                di.name
                                            );
                                        }

                                        Ok(QueryResult::Ok)
                                    })
                                }

                                BoundStatement::Insert(ins) => {
                                    db_scope!(DebugLevel::Debug, Component::Storage, "INSERT", {
                                        db_info!(
                                            Component::Storage,
                                            "Inserting into table '{}'",
                                            ins.table
                                        );
                                        db_debug!(
                                            Component::Storage,
                                            "Number of rows to insert: {}",
                                            ins.rows.len()
                                        );

                                        let mut rows = Vec::new();

                                        // Process each row
                                        for (row_idx, expr_row) in ins.rows.into_iter().enumerate()
                                        {
                                            db_trace!(
                                                Component::Storage,
                                                "Processing row {}",
                                                row_idx + 1
                                            );
                                            let mut row = Vec::new();

                                            // Process each value in the row
                                            for (col_idx, expr) in expr_row.into_iter().enumerate()
                                            {
                                                match expr {
                                                    Expr::Literal(v) => {
                                                        db_trace!(
                                                            Component::Storage,
                                                            "  Column {}: {:?}",
                                                            col_idx + 1,
                                                            v
                                                        );
                                                        row.push(v);
                                                    }
                                                    _ => {
                                                        db_error!(
                                                            Component::Storage,
                                                            "Non-literal expression in INSERT at row {}, column {}",
                                                            row_idx + 1,
                                                            col_idx + 1
                                                        );
                                                        return Err(QueryError::Exec(
                                                            anyhow::anyhow!(
                                                                "non-literal in INSERT"
                                                            ),
                                                        ));
                                                    }
                                                }
                                            }

                                            rows.push(row);
                                        }

                                        db_debug!(
                                            Component::Storage,
                                            "Inserting {} row(s)",
                                            rows.len()
                                        );
                                        self.insert_values(&ins.table, rows)?;

                                        db_info!(
                                            Component::Storage,
                                            "Insert completed successfully"
                                        );
                                        Ok(QueryResult::Ok)
                                    })
                                }

                                BoundStatement::Delete(del) => {
                                    db_scope!(DebugLevel::Debug, Component::Executor, "DELETE", {
                                        db_info!(
                                            Component::Executor,
                                            "Deleting from table '{}'",
                                            del.table
                                        );
                                        if let Some(ref pred) = del.predicate {
                                            db_debug!(
                                                Component::Executor,
                                                "WHERE clause: {:?}",
                                                pred
                                            );
                                        } else {
                                            db_warn!(
                                                Component::Executor,
                                                "No WHERE clause - deleting all rows!"
                                            );
                                        }

                                        execute_delete(del, &self.catalog)?;

                                        db_info!(
                                            Component::Executor,
                                            "Delete completed successfully"
                                        );
                                        Ok(QueryResult::Ok)
                                    })
                                }

                                BoundStatement::Update(upd) => {
                                    db_scope!(DebugLevel::Debug, Component::Executor, "UPDATE", {
                                        db_info!(
                                            Component::Executor,
                                            "Updating table '{}'",
                                            upd.table
                                        );
                                        db_debug!(
                                            Component::Executor,
                                            "Assignments: {:?}",
                                            upd.assignments
                                        );
                                        if let Some(ref pred) = upd.predicate {
                                            db_debug!(
                                                Component::Executor,
                                                "WHERE clause: {:?}",
                                                pred
                                            );
                                        } else {
                                            db_warn!(
                                                Component::Executor,
                                                "No WHERE clause - updating all rows!"
                                            );
                                        }

                                        execute_update(upd, &self.catalog)?;

                                        db_info!(
                                            Component::Executor,
                                            "Update completed successfully"
                                        );
                                        Ok(QueryResult::Ok)
                                    })
                                }
                            }
                        }
                    )?);

                    db_info!(Component::Planner, "Statement {} completed", stmt_idx + 1);
                }

                last_result.ok_or_else(|| {
                    db_error!(Component::Planner, "No statements to execute");
                    QueryError::Exec(anyhow::anyhow!("No statements to execute"))
                })
            })
        })?;

        if get_debug_level() >= DebugLevel::Info {
            eprintln!("\n{}", get_report());
        }

        db_info!(Component::Planner, "=== Query execution completed ===\n");

        Ok(result)
    }

    pub fn query(&mut self, sql: &str) -> Result<QueryResult, QueryError> {
        self.run_query(sql)
    }
}
