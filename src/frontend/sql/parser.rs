use crate::{
    common::value::Value,
    db_debug,
    debugger::debugger::DebugLevel,
    frontend::sql::lexer::{Token, Tokenizer},
};

use super::ast::*;

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    positions: Vec<Position>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Position {
    pub line: usize,
    pub column: usize,
}

impl std::fmt::Display for Position {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.line, self.column)
    }
}

impl Parser {
    pub fn new(input: &str) -> Self {
        let mut t = Tokenizer::new(input);
        let mut tokens = Vec::new();
        let mut positions = Vec::new();

        loop {
            let (tok, pos) = t.next_token();
            tokens.push(tok.clone());
            positions.push(pos);
            if tok == Token::EOF {
                break;
            }
        }

        Self {
            tokens,
            positions,
            pos: 0,
        }
    }

    fn peek(&self) -> &Token {
        &self.tokens[self.pos]
    }

    fn current_position(&self) -> Position {
        self.positions[self.pos]
    }
    fn peek_ahead(&self, n: usize) -> &Token {
        self.tokens.get(self.pos + n).unwrap_or(&Token::EOF)
    }

    fn next(&mut self) -> &Token {
        let tok = &self.tokens[self.pos];
        self.pos += 1;
        tok
    }
    fn expect(&mut self, expected: Token) -> Result<(), ParseError> {
        let pos = self.current_position();
        let tok = self.next().clone();
        if tok == expected {
            Ok(())
        } else {
            Err(ParseError::UnexpectedToken {
                token: tok,
                position: pos,
            })
        }
    }
    fn expect_ident(&mut self) -> Result<String, ParseError> {
        let pos = self.current_position();
        match self.next() {
            Token::Ident(s) => Ok(s.clone()),
            t => Err(ParseError::UnexpectedToken {
                token: t.clone(),
                position: pos,
            }),
        }
    }
    pub fn parse_statements(&mut self) -> Result<Vec<Statement>, ParseError> {
        let mut stmts = Vec::new();

        loop {
            let pos = self.current_position();
            // Skip any leading semicolons
            while matches!(self.peek(), Token::Semicolon) {
                self.next();
            }

            // Check for EOF
            if self.is_eof() {
                break;
            }

            // Parse statement
            stmts.push(self.parse_statement()?);

            // Expect semicolon between statements (except before EOF)
            if !self.is_eof() && !matches!(self.peek(), Token::Semicolon) {
                // Allow EOF without semicolon for last statement
                if !self.is_eof() {
                    return Err(ParseError::Expected {
                        expected: "semicolon or EOF".into(),
                        found: Some(format!("{:?}", self.peek())),
                        position: pos,
                    });
                }
            }
        }

        Ok(stmts)
    }

    pub fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        db_debug!(DebugLevel::Trace, "[PARSE] next token = {:?}", self.peek());
        let pos = self.current_position();

        match self.peek() {
            Token::Select => {
                self.next();
                Ok(Statement::Select(self.parse_select()?))
            }

            Token::Explain => {
                self.next();
                let analyze = if matches!(self.peek(), Token::Analyze) {
                    self.next();
                    true
                } else {
                    false
                };
                let stmt = Box::new(self.parse_statement()?);
                Ok(Statement::Explain { analyze, stmt })
            }

            Token::Create => {
                self.next();
                match self.peek() {
                    Token::Table => {
                        self.next();
                        Ok(Statement::CreateTable(self.parse_create_table()?))
                    }
                    Token::Index => {
                        self.next();
                        Ok(self.parse_create_index()?)
                    }
                    _ => Err(ParseError::Message {
                        message: "expected TABLE or INDEX".into(),
                        position: pos,
                    }),
                }
            }

            Token::Drop => {
                self.next();
                match self.peek() {
                    Token::Table => {
                        self.next();
                        Ok(Statement::DropTable(self.parse_drop_table()?))
                    }
                    Token::Index => {
                        self.next();
                        Ok(self.parse_drop_index()?)
                    }
                    _ => Err(ParseError::Message {
                        message: "expected TABLE or INDEX".into(),
                        position: pos,
                    }),
                }
            }

            Token::Insert => {
                self.next();
                Ok(Statement::Insert(self.parse_insert()?))
            }

            Token::Update => {
                self.next();
                Ok(Statement::Update(self.parse_update()?))
            }

            Token::Delete => {
                self.next();
                Ok(Statement::Delete(self.parse_delete()?))
            }

            t => Err(ParseError::UnexpectedToken {
                token: t.clone(),
                position: pos,
            }),
        }
    }

    fn parse_select(&mut self) -> Result<SelectStmt, ParseError> {
        let columns = self.parse_select_list()?;
        self.expect(Token::From)?;
        let from = self.parse_from()?;
        let pos = self.current_position();

        let where_clause = if matches!(self.peek(), Token::Where) {
            self.next();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let order_by = if matches!(self.peek(), Token::Order) {
            self.next();
            self.expect(Token::By)?;
            self.parse_order_by()?
        } else {
            Vec::new()
        };

        let limit = if matches!(self.peek(), Token::Limit) {
            self.next();
            match self.next() {
                Token::Int(n) => Some(*n as usize),
                t => {
                    return Err(ParseError::UnexpectedToken {
                        token: t.clone(),
                        position: pos,
                    });
                }
            }
        } else {
            None
        };
        Ok(SelectStmt {
            columns,
            from,
            where_clause,
            order_by,
            limit,
        })
    }
    fn parse_order_by(&mut self) -> Result<Vec<OrderByExpr>, ParseError> {
        let mut out = Vec::new();

        loop {
            let expr = self.parse_expr()?;
            let asc = if matches!(self.peek(), Token::Desc) {
                self.next();
                false
            } else if matches!(self.peek(), Token::Asc) {
                self.next();
                true
            } else {
                true
            };

            out.push(OrderByExpr { expr, asc });

            if matches!(self.peek(), Token::Comma) {
                self.next();
            } else {
                break;
            }
        }

        Ok(out)
    }
    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_and()?;

        while matches!(self.peek(), Token::Or) {
            self.next();
            let right = self.parse_and()?;
            left = Expr::Binary {
                left: Box::new(left),
                op: BinaryOp::Or,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_comparison()?;

        while matches!(self.peek(), Token::And) {
            self.next();
            let right = self.parse_comparison()?;
            left = Expr::Binary {
                left: Box::new(left),
                op: BinaryOp::And,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_arithmetic(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_term()?; // Changed from parse_primary

        while matches!(self.peek(), Token::Plus | Token::Minus) {
            let op = match self.next() {
                Token::Plus => BinaryOp::Add,
                Token::Minus => BinaryOp::Sub,
                _ => unreachable!(),
            };
            let right = self.parse_term()?; // Changed from parse_primary
            left = Expr::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    // Add this new method:
    fn parse_term(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_primary()?;

        while matches!(self.peek(), Token::Star | Token::Slash) {
            let op = match self.next() {
                Token::Star => BinaryOp::Mul,
                Token::Slash => BinaryOp::Div,
                _ => unreachable!(),
            };
            let right = self.parse_primary()?;
            left = Expr::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        let pos = self.current_position();

        match self.next() {
            Token::True => Ok(Expr::Literal(Value::Bool(true))),
            Token::False => Ok(Expr::Literal(Value::Bool(false))),
            Token::Null => Ok(Expr::Literal(Value::Null)),

            Token::Ident(first) => {
                let first = first.clone();
                if matches!(self.peek(), Token::Dot) {
                    self.next();
                    let second = self.expect_ident()?;
                    Ok(Expr::Column {
                        table: Some(first),
                        name: second,
                    })
                } else {
                    Ok(Expr::Column {
                        table: None,
                        name: first,
                    })
                }
            }

            Token::Int(n) => Ok(Expr::Literal(Value::Int64(*n))),
            Token::String(s) => Ok(Expr::Literal(Value::String(s.clone()))),

            Token::LParen => {
                let e = self.parse_expr()?;
                self.expect(Token::RParen)?;
                Ok(e)
            }

            t => Err(ParseError::UnexpectedToken {
                token: t.clone(),
                position: pos,
            }),
        }
    }

    fn parse_create_index(&mut self) -> Result<Statement, ParseError> {
        let name = self.expect_ident()?;
        self.expect(Token::On)?;
        let table = self.expect_ident()?;
        self.expect(Token::LParen)?;
        let column = self.expect_ident()?;
        self.expect(Token::RParen)?;
        Ok(Statement::CreateIndex {
            name,
            table,
            column,
        })
    }

    fn parse_drop_index(&mut self) -> Result<Statement, ParseError> {
        let name = self.expect_ident()?;
        Ok(Statement::DropIndex { name })
    }

    fn parse_select_list(&mut self) -> Result<Vec<SelectItem>, ParseError> {
        let mut cols = Vec::with_capacity(4);

        loop {
            match self.peek() {
                Token::Star => {
                    self.next();
                    cols.push(SelectItem {
                        expr: Expr::Column {
                            table: None,
                            name: "*".into(),
                        },
                        alias: None,
                    });
                }
                _ => {
                    let expr = self.parse_expr()?;

                    // Optional alias
                    let alias = if self.peek().is_keyword("AS") {
                        self.next(); // consume AS
                        Some(self.expect_ident()?)
                    } else {
                        None
                    };

                    db_debug!(
                        DebugLevel::Debug,
                        "[PARSE] select item: expr={:?}, alias={:?}",
                        expr,
                        alias
                    );

                    cols.push(SelectItem { expr, alias });
                }
            }

            if matches!(self.peek(), Token::Comma) {
                self.next();
            } else {
                break;
            }
        }
        Ok(cols)
    }
    fn parse_from(&mut self) -> Result<FromItem, ParseError> {
        // 1. Parse the leftmost table
        let mut left = self.parse_table_ref()?;

        // 2. Parse zero or more JOIN clauses
        loop {
            if matches!(self.peek(), Token::Join) {
                self.next(); // consume JOIN

                let right = self.parse_table_ref()?;

                self.expect(Token::On)?;
                let on = self.parse_expr()?;

                left = FromItem::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    on,
                };
            } else {
                break;
            }
        }

        Ok(left)
    }

    fn parse_create_table(&mut self) -> Result<CreateTableStmt, ParseError> {
        let table_name = self.expect_ident()?;
        self.expect(Token::LParen)?;

        let mut columns = Vec::new();

        loop {
            let name = self.expect_ident()?;
            let pos = self.current_position();
            let ty = self.expect_ident()?; // INT, TEXT, BOOL

            let sql_ty = match ty.to_uppercase().as_str() {
                "INT" => SqlType::Int,
                "TEXT" => SqlType::Text,
                "BOOL" => SqlType::Bool,
                _ => {
                    return Err(ParseError::Message {
                        message: format!("unknown type '{}'", ty),
                        position: pos,
                    });
                }
            };

            let mut nullable = true;
            if matches!(self.peek(), Token::Not) {
                self.next();
                self.expect(Token::Null)?;
                nullable = false;
            } else if matches!(self.peek(), Token::Null) {
                self.next();
                nullable = true;
            }

            columns.push(ColumnDef {
                name,
                ty: sql_ty,
                nullable,
            });

            if matches!(self.peek(), Token::Comma) {
                self.next();
            } else {
                break;
            }
        }

        self.expect(Token::RParen)?;

        Ok(CreateTableStmt {
            table_name,
            columns,
        })
    }

    fn parse_drop_table(&mut self) -> Result<DropTableStmt, ParseError> {
        let table_name = self.expect_ident()?;
        Ok(DropTableStmt { table_name })
    }

    fn parse_insert(&mut self) -> Result<InsertStmt, ParseError> {
        self.expect(Token::Into)?;
        let table = self.expect_ident()?;
        self.expect(Token::Values)?;

        let mut rows = Vec::new();

        loop {
            self.expect(Token::LParen)?;
            let mut values = Vec::new();

            loop {
                values.push(self.parse_expr()?);
                if matches!(self.peek(), Token::Comma) {
                    self.next();
                } else {
                    break;
                }
            }

            self.expect(Token::RParen)?;
            rows.push(values);

            // Check for more rows
            if matches!(self.peek(), Token::Comma) {
                self.next();
            } else {
                break;
            }
        }

        Ok(InsertStmt { table, rows }) // Change values to rows
    }

    fn parse_update(&mut self) -> Result<UpdateStmt, ParseError> {
        let table = self.expect_ident()?;
        self.expect(Token::Set)?;

        let mut assignments = Vec::new();

        loop {
            let col = self.expect_ident()?;
            self.expect(Token::Eq)?;
            let expr = self.parse_expr()?;

            assignments.push((col, expr));
            if matches!(self.peek(), Token::Comma) {
                self.next();
            } else {
                break;
            }
        }

        let where_clause = if matches!(self.peek(), Token::Where) {
            self.next();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(UpdateStmt {
            table,
            assignments,
            where_clause,
        })
    }

    fn parse_delete(&mut self) -> Result<DeleteStmt, ParseError> {
        self.expect(Token::From)?;
        let table = self.expect_ident()?;

        let where_clause = if matches!(self.peek(), Token::Where) {
            self.next();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(DeleteStmt {
            table,
            where_clause,
        })
    }

    fn parse_table_ref(&mut self) -> Result<FromItem, ParseError> {
        let name = self.expect_ident()?;

        let alias = if let Token::Ident(_) = self.peek() {
            // Also check it's not a keyword
            if matches!(
                self.peek(),
                Token::Where | Token::Order | Token::Limit | Token::Join
            ) {
                None
            } else {
                Some(self.expect_ident()?)
            }
        } else {
            None
        };
        Ok(FromItem::Table { name, alias })
    }

    fn parse_comparison(&mut self) -> Result<Expr, ParseError> {
        if matches!(self.peek(), Token::Not) {
            self.next();
            let expr = self.parse_comparison()?;
            return Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(expr),
            });
        }

        let mut left = self.parse_arithmetic()?;

        let op = match self.peek() {
            Token::Eq => BinaryOp::Eq,
            Token::NotEq => BinaryOp::Neq,
            Token::Lt => BinaryOp::Lt,
            Token::Le => BinaryOp::Lte,
            Token::Gt => BinaryOp::Gt,
            Token::Ge => BinaryOp::Gte,
            _ => return Ok(left),
        };

        self.next(); // consume operator
        let right = self.parse_arithmetic()?;

        Ok(Expr::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    }
}

impl Parser {
    #[inline]
    pub fn is_eof(&self) -> bool {
        matches!(self.peek(), Token::EOF)
    }
}
impl Token {
    pub fn is_keyword(&self, kw: &str) -> bool {
        matches!(self, Token::Ident(s) if s.eq_ignore_ascii_case(kw))
    }
}
