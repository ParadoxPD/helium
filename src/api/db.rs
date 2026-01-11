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
    pub fn new(path: &PathBuf) -> Result<Self, DbError> {
        let pm = FilePageManager::open(path)?;
        Ok(Self {
            catalog: Catalog::new(),
            buffer_pool: BufferPoolHandle::new(Mutex::new(BufferPool::new(Box::new(pm)))),
        })
    }

    /// Execute a SQL statement
    pub fn execute(
        &mut self,
        stmt: crate::frontend::sql::ast::Statement,
    ) -> Result<ExecutionResult, DbError> {
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

        match optimized {
            crate::ir::plan::LogicalPlan::Insert { .. }
            | crate::ir::plan::LogicalPlan::Update { .. }
            | crate::ir::plan::LogicalPlan::Delete { .. } => {
                Ok(execute_mutation(optimized, &mut ctx)?)
            }

            _ => Ok(execute_query(optimized, &mut ctx)?),
        }
    }
}

