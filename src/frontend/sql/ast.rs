use std::fmt;

use crate::common::value::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStmt),
    Insert(InsertStmt),
    Delete(DeleteStmt),
    Update(UpdateStmt),

    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
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
pub struct CreateTableStmt {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub ty: SqlType,
    pub nullable: bool, // default = true
}

#[derive(Debug, Clone, PartialEq)]
pub enum SqlType {
    Int,
    Bool,
    Text,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStmt {
    pub table_name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStmt {
    pub table: String,
    pub assignments: Vec<(String, Expr)>,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
    pub table: String,
    pub values: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStmt {
    pub table: String,
    pub where_clause: Option<Expr>,
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

#[derive(Debug, Clone)]
pub enum ParseError {
    UnexpectedEOF,
    UnexpectedToken(String),
    Expected {
        expected: String,
        found: Option<String>,
    },
    Unsupported(String),
    InvalidLiteral(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::UnexpectedEOF => write!(f, "Unexpected end of file"),
            ParseError::UnexpectedToken(tok) => write!(f, "Unexpected token : {}", tok),
            ParseError::Expected { expected, found } => {
                write!(f, "Expected : {:?}, Found : {:?}", expected, found)
            }
            ParseError::Unsupported(tok) => write!(f, "Unsupported operation : {}", tok),
            ParseError::InvalidLiteral(tok) => write!(f, "Invalid Literal : {}", tok),
        }
    }
}
