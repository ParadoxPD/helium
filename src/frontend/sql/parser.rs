use crate::{
    common::value::Value,
    frontend::sql::lexer::{Token, Tokenizer},
};

use super::ast::*;

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    pub fn new(input: &str) -> Self {
        let mut t = Tokenizer::new(input);
        let mut tokens = Vec::new();
        loop {
            let tok = t.next_token();
            tokens.push(tok.clone());
            if tok == Token::EOF {
                break;
            }
        }

        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> &Token {
        &self.tokens[self.pos]
    }

    fn next(&mut self) -> &Token {
        let tok = &self.tokens[self.pos];
        self.pos += 1;
        tok
    }

    fn expect(&mut self, expected: Token) -> Result<(), ParseError> {
        let tok = self.next().clone();
        if tok == expected {
            Ok(())
        } else {
            Err(ParseError::UnexpectedToken(tok))
        }
    }

    fn expect_ident(&mut self) -> Result<String, ParseError> {
        match self.next() {
            Token::Ident(s) => Ok(s.clone()),
            t => Err(ParseError::UnexpectedToken(t.clone())),
        }
    }

    fn match_keyword(&mut self, kw: &str) -> bool {
        matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case(kw))
    }

    fn consume_keyword(&mut self, kw: &str) -> Result<(), ParseError> {
        if self.match_keyword(kw) {
            self.next();
            Ok(())
        } else {
            Err(ParseError::Message(format!("expected keyword {}", kw)))
        }
    }

    pub fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        match self.peek() {
            Token::Ident(s) if s.eq_ignore_ascii_case("SELECT") => {
                self.next();
                Ok(Statement::Select(self.parse_select()?))
            }

            Token::Ident(s) if s.eq_ignore_ascii_case("EXPLAIN") => {
                self.next();
                let analyze = if self.match_keyword("ANALYZE") {
                    self.next();
                    true
                } else {
                    false
                };
                let stmt = Box::new(self.parse_statement()?);
                Ok(Statement::Explain { analyze, stmt })
            }

            Token::Ident(s) if s.eq_ignore_ascii_case("CREATE") => {
                self.next();
                if self.match_keyword("TABLE") {
                    self.next();
                    Ok(Statement::CreateTable(self.parse_create_table()?))
                } else if self.match_keyword("INDEX") {
                    self.next();
                    Ok(self.parse_create_index()?)
                } else {
                    Err(ParseError::Message("expected TABLE or INDEX".into()))
                }
            }

            Token::Ident(s) if s.eq_ignore_ascii_case("DROP") => {
                self.next();
                if self.match_keyword("TABLE") {
                    self.next();
                    Ok(Statement::DropTable(self.parse_drop_table()?))
                } else if self.match_keyword("INDEX") {
                    self.next();
                    Ok(self.parse_drop_index()?)
                } else {
                    Err(ParseError::Message("expected TABLE or INDEX".into()))
                }
            }

            Token::Ident(s) if s.eq_ignore_ascii_case("INSERT") => {
                self.next();
                Ok(Statement::Insert(self.parse_insert()?))
            }

            Token::Ident(s) if s.eq_ignore_ascii_case("UPDATE") => {
                self.next();
                Ok(Statement::Update(self.parse_update()?))
            }

            Token::Ident(s) if s.eq_ignore_ascii_case("DELETE") => {
                self.next();
                Ok(Statement::Delete(self.parse_delete()?))
            }

            t => Err(ParseError::UnexpectedToken(t.clone())),
        }
    }
    fn parse_select(&mut self) -> Result<SelectStmt, ParseError> {
        let columns = self.parse_select_list()?;
        self.consume_keyword("FROM")?;
        let from = self.parse_from()?;

        let where_clause = if self.match_keyword("WHERE") {
            self.next();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let order_by = if self.match_keyword("ORDER") {
            self.next();
            self.consume_keyword("BY")?;
            self.parse_order_by()?
        } else {
            Vec::new()
        };

        let limit = if self.match_keyword("LIMIT") {
            self.next();
            match self.next() {
                Token::Int(n) => Some(*n as usize),
                t => return Err(ParseError::UnexpectedToken(t.clone())),
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
            let asc = if self.match_keyword("DESC") {
                self.next();
                false
            } else if self.match_keyword("ASC") {
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

        while self.match_keyword("OR") {
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

        while self.match_keyword("AND") {
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
        // Take ownership of the token FIRST
        let tok = self.next().clone();

        match tok {
            Token::Ident(first) => {
                if first.eq_ignore_ascii_case("NULL") {
                    return Ok(Expr::Literal(Value::Null));
                }

                if first.eq_ignore_ascii_case("TRUE") {
                    return Ok(Expr::Literal(Value::Bool(true)));
                }
                if first.eq_ignore_ascii_case("FALSE") {
                    return Ok(Expr::Literal(Value::Bool(false)));
                }

                // Now it's safe to inspect / mutate self again
                if matches!(self.peek(), Token::Dot) {
                    self.next(); // consume '.'
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

            Token::Int(n) => Ok(Expr::Literal(Value::Int64(n))),

            Token::String(s) => Ok(Expr::Literal(Value::String(s))),

            Token::LParen => {
                let e = self.parse_expr()?;
                self.expect(Token::RParen)?;
                Ok(e)
            }

            t => Err(ParseError::UnexpectedToken(t)),
        }
    }

    fn parse_create_index(&mut self) -> Result<Statement, ParseError> {
        let name = self.expect_ident()?;
        self.consume_keyword("ON")?;
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

    fn parse_select_list(&mut self) -> Result<Vec<Expr>, ParseError> {
        let mut cols = Vec::new();

        loop {
            match self.peek() {
                Token::Star => {
                    self.next();
                    cols.push(Expr::Column {
                        table: None,
                        name: "*".into(),
                    });
                }
                _ => {
                    let expr = self.parse_expr()?;
                    cols.push(expr);
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
            if self.match_keyword("JOIN") {
                self.next(); // consume JOIN

                let right = self.parse_table_ref()?;

                self.consume_keyword("ON")?;
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
            let ty = self.expect_ident()?; // INT, TEXT, BOOL

            let sql_ty = match ty.to_uppercase().as_str() {
                "INT" => SqlType::Int,
                "TEXT" => SqlType::Text,
                "BOOL" => SqlType::Bool,
                _ => return Err(ParseError::Message("unknown type".into())),
            };

            let mut nullable = true;
            if self.match_keyword("NOT") {
                self.next();
                self.consume_keyword("NULL")?;
                nullable = false;
            } else if self.match_keyword("NULL") {
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
        self.consume_keyword("INTO")?;
        let table = self.expect_ident()?;
        self.consume_keyword("VALUES")?;

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
        self.consume_keyword("SET")?;

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

        let where_clause = if self.match_keyword("WHERE") {
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
        self.consume_keyword("FROM")?;
        let table = self.expect_ident()?;

        let where_clause = if self.match_keyword("WHERE") {
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

        let alias = if matches!(self.peek(), Token::Ident(_)) {
            Some(self.expect_ident()?)
        } else {
            None
        };

        Ok(FromItem::Table { name, alias })
    }

    fn parse_comparison(&mut self) -> Result<Expr, ParseError> {
        if self.match_keyword("NOT") {
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
