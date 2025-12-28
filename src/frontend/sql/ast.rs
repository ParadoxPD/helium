#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStmt),
    Explain { analyze: bool, stmt: Box<Statement> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub columns: Vec<String>,
    pub from: FromItem,
    pub where_clause: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(String),
    LiteralInt(i64),
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr {
    pub column: String,
    pub asc: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinaryOp {
    Eq,
    Gt,
    Lt,
    And,
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
