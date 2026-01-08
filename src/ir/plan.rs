//! Logical query plan representation.
//!
//! This module is FROZEN.

use crate::ir::expr::Expr;

pub type TableId = u32;
pub type IndexId = u32;

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum LogicalPlan {
    Scan {
        table_id: TableId,
    },

    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },

    Project {
        input: Box<LogicalPlan>,
        exprs: Vec<Expr>,
    },

    Sort {
        input: Box<LogicalPlan>,
        keys: Vec<SortKey>,
    },

    Limit {
        input: Box<LogicalPlan>,
        limit: u64,
        offset: u64,
    },

    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        on: Expr,
        join_type: JoinType,
    },

    IndexScan {
        table_id: TableId,
        index_id: IndexId,
        predicate: Expr,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct SortKey {
    pub expr: Expr,
    pub asc: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}
