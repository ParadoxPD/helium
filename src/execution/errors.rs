use core::fmt;

use crate::{
    api::errors::{DefinitionResult, MutationResult, QueryResult},
    binder::bound::BoundExpr,
    catalog::ids::{IndexId, TableId},
    storage::errors::StorageError,
    types::value::Value,
};

pub type ExecutionResultType = Result<ExecutionResult, ExecutionError>;

#[derive(Debug)]
pub enum ExecutionResult {
    /// SELECT, INDEX SCAN, JOIN, etc.
    Query(QueryResult),

    /// INSERT / UPDATE / DELETE
    Mutation(MutationResult),

    /// CREATE / DROP (future-proof)
    Definition(DefinitionResult),
}

#[derive(Debug, Default, Clone)]
pub struct ExecutionStats {
    /// Rows produced (for queries)
    pub rows_output: u64,

    /// Rows scanned from storage
    pub rows_scanned: u64,

    /// Rows filtered out
    pub rows_filtered: u64,

    /// Index lookups performed
    pub index_lookups: u64,

    /// Storage operations performed
    pub storage_ops: u64,
}

#[derive(Debug, Clone)]
pub struct TableMutationStats {
    /// Target table
    pub table_id: TableId,

    /// Number of rows affected in this table
    pub rows_affected: u64,

    /// Number of physical row writes performed
    /// (insert/update/delete at storage layer)
    pub rows_written: u64,

    /// Number of rows logically deleted (for UPDATE/DELETE)
    pub rows_deleted: u64,

    /// Number of indexes updated for this table
    pub indexes_updated: usize,

    /// Per-index mutation breakdown (optional but powerful)
    pub per_index: Vec<IndexMutationStats>,
}

#[derive(Debug, Clone)]
pub struct IndexMutationStats {
    pub index_id: IndexId,

    /// Number of index entries inserted
    pub entries_inserted: u64,

    /// Number of index entries removed
    pub entries_deleted: u64,
}

impl TableMutationStats {
    pub fn new(table_id: TableId) -> Self {
        Self {
            table_id,
            rows_affected: 0,
            rows_written: 0,
            rows_deleted: 0,
            indexes_updated: 0,
            per_index: Vec::new(),
        }
    }

    pub fn record_index_insert(&mut self, index_id: IndexId) {
        self.indexes_updated += 1;
        if let Some(stats) = self.per_index.iter_mut().find(|s| s.index_id == index_id) {
            stats.entries_inserted += 1;
        } else {
            self.per_index.push(IndexMutationStats {
                index_id,
                entries_inserted: 1,
                entries_deleted: 0,
            });
        }
    }

    pub fn record_index_delete(&mut self, index_id: IndexId) {
        self.indexes_updated += 1;
        if let Some(stats) = self.per_index.iter_mut().find(|s| s.index_id == index_id) {
            stats.entries_deleted += 1;
        } else {
            self.per_index.push(IndexMutationStats {
                index_id,
                entries_inserted: 0,
                entries_deleted: 1,
            });
        }
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum ExecutionError {
    // ----------------------------
    // Catalog / Resolution errors
    // ----------------------------
    TableNotFound {
        table_id: TableId,
    },

    IndexNotFound {
        index_id: IndexId,
    },

    ColumnOutOfBounds {
        index: usize,
        column_count: usize,
    },

    // ----------------------------
    // Plan / Engine errors
    // ----------------------------
    InvalidPlan {
        reason: String,
    },

    ExecutorInvariantViolation {
        reason: String,
    },

    // ----------------------------
    // Expression evaluation
    // ----------------------------
    ExpressionError {
        message: String,
    },

    TypeError {
        expected: String,
        found: Value,
    },

    InvalidExpression {
        reason: String,
    },
    DivisionByZero,

    TypeMismatch {
        op: String,
        left: Value,
        right: Value,
    },
    UnboundColumn,

    // ----------------------------
    // Index & constraint errors
    // ----------------------------
    IndexViolation {
        index_id: IndexId,
        reason: String,
    },

    // ----------------------------
    // Storage passthrough (boxed)
    // ----------------------------
    Storage(StorageError),
    Internal(String),
}

impl From<StorageError> for ExecutionError {
    fn from(err: StorageError) -> Self {
        ExecutionError::Storage(err)
    }
}

impl ExecutionError {
    pub fn index_key_error(index_id: IndexId, msg: impl Into<String>) -> Self {
        ExecutionError::IndexViolation {
            index_id,
            reason: msg.into(),
        }
    }
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutionError::TableNotFound { table_id } => {
                write!(f, "table {:?} does not exist", table_id)
            }
            ExecutionError::IndexNotFound { index_id } => {
                write!(f, "index {:?} does not exist", index_id)
            }
            ExecutionError::InvalidPlan { reason } => {
                write!(f, "invalid execution plan: {}", reason)
            }
            ExecutionError::ExecutorInvariantViolation { reason } => {
                write!(f, "executor invariant violated: {}", reason)
            }
            ExecutionError::ExpressionError { message } => {
                write!(f, "expression evaluation error: {}", message)
            }
            ExecutionError::TypeError { expected, found } => {
                write!(f, "type error: expected {}, found {:?}", expected, found)
            }
            ExecutionError::IndexViolation { index_id, reason } => {
                write!(f, "index {:?} violation: {}", index_id, reason)
            }
            ExecutionError::ColumnOutOfBounds {
                index,
                column_count,
            } => write!(
                f,
                "column index {} out of bounds ({} columns)",
                index, column_count
            ),
            ExecutionError::Storage(err) => write!(f, "storage error: {}", err),
            ExecutionError::InvalidExpression { reason } => {
                write!(f, "invalid expression: {}", reason)
            }
            ExecutionError::DivisionByZero => write!(f, "division by zero"),
            ExecutionError::TypeMismatch { op, left, right } => {
                write!(f, "type mismatch in {}: {:?} and {:?}", op, left, right)
            }
            ExecutionError::UnboundColumn => write!(f, "unbound column reference"),
            ExecutionError::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for ExecutionError {}
