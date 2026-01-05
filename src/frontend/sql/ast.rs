use std::fmt;

use crate::{
    common::value::Value,
    frontend::sql::{lexer::Token, parser::Position},
};

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
    pub columns: Vec<SelectItem>,
    pub from: FromItem,
    pub where_clause: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectItem {
    pub expr: Expr,
    pub alias: Option<String>,
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
    pub rows: Vec<Vec<Expr>>,
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
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Minus,
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
    UnexpectedEOF {
        position: Position,
    },
    UnexpectedToken {
        token: Token,
        position: Position,
    },
    Expected {
        expected: String,
        found: Option<String>,
        position: Position,
    },
    Unsupported {
        message: String,
        position: Position,
    },
    InvalidLiteral {
        literal: String,
        position: Position,
    },
    Message {
        message: String,
        position: Position,
    },
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::UnexpectedEOF { position } => {
                write!(f, "Unexpected end of file at {}", position)
            }
            ParseError::UnexpectedToken { token, position } => {
                write!(f, "Unexpected token {:?} at {}", token, position)
            }
            ParseError::Expected {
                expected,
                found,
                position,
            } => {
                write!(
                    f,
                    "Expected {} but found {:?} at {}",
                    expected, found, position
                )
            }
            ParseError::Unsupported { message, position } => {
                write!(f, "Unsupported: {} at {}", message, position)
            }
            ParseError::InvalidLiteral { literal, position } => {
                write!(f, "Invalid literal '{}' at {}", literal, position)
            }
            ParseError::Message { message, position } => {
                write!(f, "{} at {}", message, position)
            }
        }
    }
}
