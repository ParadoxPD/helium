use crate::{
    binder::errors::BindError,
    execution::{
        errors::{ExecutionError, ExecutionStats, TableMutationStats},
        executor::Row,
    },
    frontend::sql::errors::ParseError,
    optimizer::errors::OptimizerError,
    planner::errors::PlanError,
    storage::errors::StorageError,
    types::schema::Schema,
};

#[derive(Debug)]
pub struct QueryResult {
    /// Column names and types (post-projection)
    pub schema: Schema,

    /// Materialized rows (phase 1)
    pub rows: Vec<Row>,

    /// Execution metadata
    pub stats: ExecutionStats,
}

#[derive(Debug)]
pub struct MutationResult {
    pub kind: MutationKind,

    /// Number of rows affected
    pub rows_affected: u64,

    /// Per-table breakdown (future-proof)
    pub per_table: Vec<TableMutationStats>,

    /// Execution metadata
    pub stats: ExecutionStats,
}

#[derive(Debug)]
pub enum MutationKind {
    Insert,
    Update,
    Delete,
}

#[derive(Debug)]
pub struct DefinitionResult {
    pub action: DefinitionAction,
    pub object: String,
    pub stats: ExecutionStats,
}

#[derive(Debug)]
pub enum DefinitionAction {
    CreateTable,
    DropTable,
    CreateIndex,
    DropIndex,
}

#[derive(Debug)]
pub enum DbError {
    Parse(ParseError),
    Bind(BindError),
    Plan(PlanError),
    Optimize(OptimizerError),
    Execution(ExecutionError),
    Storage(StorageError),
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::Parse(e) => write!(f, "Parse error: {}", e),
            DbError::Bind(e) => write!(f, "Bind error: {}", e),
            DbError::Plan(e) => write!(f, "planner error: {e}"),
            DbError::Optimize(e) => write!(f, "optimizer error: {e}"),
            DbError::Execution(e) => write!(f, "execution error: {e}"),
            DbError::Storage(e) => write!(f, "storage error: {e}"),
        }
    }
}

impl std::error::Error for DbError {}

impl From<ParseError> for DbError {
    fn from(e: ParseError) -> Self {
        DbError::Parse(e)
    }
}

impl From<BindError> for DbError {
    fn from(e: BindError) -> Self {
        DbError::Bind(e)
    }
}

impl From<PlanError> for DbError {
    fn from(e: PlanError) -> Self {
        DbError::Plan(e)
    }
}

impl From<OptimizerError> for DbError {
    fn from(e: OptimizerError) -> Self {
        DbError::Optimize(e)
    }
}

impl From<ExecutionError> for DbError {
    fn from(e: ExecutionError) -> Self {
        DbError::Execution(e)
    }
}

impl From<StorageError> for DbError {
    fn from(e: StorageError) -> Self {
        DbError::Storage(e)
    }
}
