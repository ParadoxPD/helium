use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use crate::{
    binder::errors::BindError,
    catalog::catalog::Catalog,
    execution::executor::Row,
    frontend::sql::ast::ParseError,
    storage::{
        buffer::pool::{BufferPool, BufferPoolHandle},
        pagemgr::file::FilePageManager,
    },
};

#[derive(Debug)]
pub enum QueryResult {
    Ok(String),
    Rows(Vec<Row>),
    Explain(String),
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

    pub fn run_query(&mut self, sql: &str) -> Result<QueryResult, QueryError> {
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

                                            let optimized = db_phase!("Optimize Plan", {
                                                db_debug!(
                                                    Component::Optimizer,
                                                    "Starting optimization"
                                                );
                                                let optimized = optimize(&plan, &self.catalog);
                                                db_info!(
                                                    Component::Optimizer,
                                                    "Optimized plan:\n{}",
                                                    pretty(&optimized)
                                                );
                                                optimized
                                            });

                                            // Execute
                                            db_phase!("Execute Query", {
                                                db_debug!(
                                                    Component::Executor,
                                                    "Creating execution tree"
                                                );
                                                execute_plan(optimized, &self.catalog)
                                                    .map_err(QueryError::Exec)
                                                    .and_then(Self::handle_result)
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

                                                        let row_count = 0;
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
                                            Ok(QueryResult::Ok("Table Created successfully".into()))
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
                                            Ok(QueryResult::Ok("Table Dropped successfully".into()))
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
                                            Ok(QueryResult::Ok("Index Created successfully".into()))
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

                                        Ok(QueryResult::Ok("Index Dropped successfully".into()))
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

                                        let rows = self.insert_values(&ins.table, rows)?;

                                        db_info!(
                                            Component::Storage,
                                            "Insert completed successfully"
                                        );
                                        Ok(QueryResult::Ok(format!(
                                            "{} rows inserted successfully",
                                            rows
                                        )))
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

                                        let rows = execute_delete(del, &self.catalog)?;

                                        db_info!(
                                            Component::Executor,
                                            "Delete completed successfully"
                                        );
                                        Ok(QueryResult::Ok(format!(
                                            "{} rows deleted successfully",
                                            rows
                                        )))
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

                                        let rows = execute_update(upd, &self.catalog)?;

                                        db_info!(
                                            Component::Executor,
                                            "Update completed successfully"
                                        );
                                        Ok(QueryResult::Ok(format!(
                                            "{} rows deleted successfully",
                                            rows
                                        )))
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

    fn log_rows(component: Component, rows: &[Row]) {
        db_debug!(component, "Fetching rows");

        for (i, row) in rows.iter().enumerate() {
            db_trace!(component, "Row {}: {:?}", i + 1, row);
        }

        db_info!(component, "Query returned {} row(s)", rows.len());
    }

    fn handle_result(res: QueryResult) -> Result<QueryResult, QueryError> {
        match &res {
            QueryResult::Rows(rows) => Self::log_rows(Component::Executor, rows),
            QueryResult::Explain(plan) => {
                db_info!(Component::Executor, "{:?}", plan)
            }
            QueryResult::Ok(message) => {
                db_info!(Component::Executor, "{}", message)
            }
        }
        Ok(res)
    }
}
