use crate::execution::context::ExecutionContext;
use crate::execution::errors::{
    ExecutionError, ExecutionResult, ExecutionResultType, TableMutationStats,
};
use crate::types::value::Value;

pub type Row = Vec<Value>;
pub type ExecResult<T> = Result<T, ExecutionError>;

pub trait Executor {
    /// Prepare executor (allocate cursors, open children, etc.)
    fn open(&mut self, ctx: &mut ExecutionContext) -> ExecResult<()>;

    /// Produce the next row (DQL only)
    ///
    /// - Ok(Some(row)) → row produced
    /// - Ok(None)      → end of stream
    /// - Err(e)        → execution/storage error
    fn next(&mut self, ctx: &mut ExecutionContext) -> ExecResult<Option<Row>>;

    /// Finalize execution and return mutation statistics (DML/DDL)
    ///
    /// - For SELECT: returns empty vec
    /// - For INSERT/UPDATE/DELETE: returns per-table stats
    fn close(&mut self, ctx: &mut ExecutionContext) -> ExecResult<Vec<TableMutationStats>>;
}
