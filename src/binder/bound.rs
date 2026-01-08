//! Bound AST produced by the binder.
//!
//! Fully resolved, planner-facing representation.

use crate::catalog::ids::{IndexId, TableId};
use crate::ir::expr::{BinaryOp, UnaryOp};
use crate::ir::plan::JoinType;
use crate::types::schema::ColumnId;
use crate::types::value::Value;

#[derive(Debug, Clone)]
pub enum BoundExpr {
    Column {
        column_id: ColumnId,
    },

    Literal(Value),

    Unary {
        op: UnaryOp,
        expr: Box<BoundExpr>,
    },

    Binary {
        left: Box<BoundExpr>,
        op: BinaryOp,
        right: Box<BoundExpr>,
    },

    Null,
}

#[derive(Debug, Clone)]
pub enum BoundFrom {
    Table {
        table_id: TableId,
    },

    Join {
        left: Box<BoundFrom>,
        right: Box<BoundFrom>,
        on: BoundExpr,
        join_type: JoinType,
    },
}

#[derive(Debug)]
pub struct BoundSelect {
    pub projection: Vec<BoundExpr>,
    pub from: BoundFrom,
    pub selection: Option<BoundExpr>,
    pub order_by: Vec<(BoundExpr, bool)>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Debug)]
pub struct BoundInsert {
    pub table_id: TableId,
    pub rows: Vec<Vec<BoundExpr>>,
}

#[derive(Debug)]
pub struct BoundUpdate {
    pub table_id: TableId,
    pub assignments: Vec<(ColumnId, BoundExpr)>,
    pub predicate: Option<BoundExpr>,
}

#[derive(Debug)]
pub struct BoundDelete {
    pub table_id: TableId,
    pub predicate: Option<BoundExpr>,
}

#[derive(Debug)]
pub struct BoundCreateTable {
    pub table_name: String,
    pub schema: crate::types::schema::Schema,
}

#[derive(Debug)]
pub struct BoundDropTable {
    pub table_id: TableId,
}

#[derive(Debug)]
pub struct BoundCreateIndex {
    pub name: String,
    pub table_id: TableId,
    pub column_id: ColumnId,
}

#[derive(Debug)]
pub struct BoundDropIndex {
    pub index_id: IndexId,
}

#[derive(Debug)]
pub enum BoundStatement {
    Select(BoundSelect),
    Insert(BoundInsert),
    Update(BoundUpdate),
    Delete(BoundDelete),

    CreateTable(BoundCreateTable),
    DropTable(BoundDropTable),
    CreateIndex(BoundCreateIndex),
    DropIndex(BoundDropIndex),

    Explain {
        analyze: bool,
        stmt: Box<BoundStatement>,
    },
}
