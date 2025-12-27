#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub columns: Vec<String>,
    pub table: String,
    pub where_clause: Option<Expr>,
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinaryOp {
    Eq,
    Gt,
    Lt,
    And,
}
