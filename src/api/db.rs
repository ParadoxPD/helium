use std::{path::PathBuf, sync::Mutex};

use crate::{
    api::errors::DbError,
    binder::bind_stmt::Binder,
    catalog::catalog::Catalog,
    execution::{
        context::ExecutionContext,
        engine::{execute_mutation, execute_query},
        errors::ExecutionResult,
    },
    frontend::sql::parser::Parser,
    optimizer::optimize,
    planner::logical::LogicalPlanner,
    storage::{
        buffer::pool::{BufferPool, BufferPoolHandle},
        pagemgr::file::FilePageManager,
    },
};

pub struct Database {
    catalog: Catalog,
    buffer_pool: BufferPoolHandle,
}

impl Database {
    pub fn new(path: String) -> Result<Self, DbError> {
        let pm = FilePageManager::open(&PathBuf::from(path))?;
        Ok(Self {
            catalog: Catalog::new(),
            buffer_pool: BufferPoolHandle::new(Mutex::new(BufferPool::new(Box::new(pm)))),
        })
    }

    /// Execute a SQL statement
    pub fn execute(&mut self, query: &str) -> Result<ExecutionResult, DbError> {
        //
        //0. Parse
        //
        let mut parser = Parser::new(query);
        let stmts = parser.parse_statements()?;
        let mut result: Option<ExecutionResult> = None;

        for stmt in stmts {
            // -------------------------
            // 1. Bind
            // -------------------------
            let binder = Binder::new(&self.catalog);
            let bound = binder.bind_statement(stmt)?;

            // -------------------------
            // 2. Plan
            // -------------------------
            let planner = LogicalPlanner::new();
            let logical = planner.plan(bound)?;

            // -------------------------
            // 3. Optimize
            // -------------------------
            let optimized = optimize(&logical, &self.catalog)?;

            // -------------------------
            // 4. Execute
            // -------------------------
            let mut ctx = ExecutionContext::new(&self.catalog, &self.buffer_pool);

            let exec_result = match optimized {
                crate::ir::plan::LogicalPlan::Insert { .. }
                | crate::ir::plan::LogicalPlan::Update { .. }
                | crate::ir::plan::LogicalPlan::Delete { .. } => {
                    execute_mutation(optimized, &mut ctx)?
                }

                _ => execute_query(optimized, &mut ctx)?,
            };

            result = Some(exec_result);
        }
        result.ok_or(DbError::EmptyQuery)
    }
}
