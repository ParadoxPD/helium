use crate::common::value::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStmt),
    Explain {
        analyze: bool,
        stmt: Box<Statement>,
    },
    CreateIndex {
        name: String,
        table: String,
        column: String,
    },
    DropIndex {
        name: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub columns: Vec<Expr>,
    pub from: FromItem,
    pub where_clause: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column {
        table: Option<String>,
        name: String,
    },
    Literal(Value),
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub asc: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    And,
    Or,
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FromItem {
    Table {
        name: String,
        alias: Option<String>,
    },
    Join {
        left: Box<FromItem>,
        right: Box<FromItem>,
        on: Expr,
    },
}
