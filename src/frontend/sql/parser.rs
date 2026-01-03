use crate::common::value::Value;

use super::ast::*;

pub fn parse(sql: &str) -> Result<Statement, ParseError> {
    let sql = sql.trim().trim_end_matches(';');
    let mut parts = sql.split_whitespace().peekable();

    match parts.peek() {
        Some(&"EXPLAIN") => parse_explain(&mut parts),
        Some(&"CREATE") => parse_create(&mut parts),
        Some(&"DROP") => parse_drop(&mut parts),
        Some(&"SELECT") => parse_select(&mut parts),
        _ => return Err(ParseError::Unsupported(sql.to_string())),
    }
}

fn parse_explain<'a, I>(parts: &mut std::iter::Peekable<I>) -> Result<Statement, ParseError>
where
    I: Iterator<Item = &'a str>,
{
    parts.next(); // EXPLAIN

    let analyze = matches!(parts.peek(), Some(&"ANALYZE"));
    if analyze {
        parts.next();
    }

    let rest = parts.collect::<Vec<_>>().join(" ");
    Ok(Statement::Explain {
        analyze,
        stmt: Box::new(parse(&rest)?),
    })
}

fn parse_create<'a, I>(parts: &mut std::iter::Peekable<I>) -> Result<Statement, ParseError>
where
    I: Iterator<Item = &'a str>,
{
    parts.next(); // CREATE

    match parts.next() {
        Some("TABLE") => parse_create_table(parts),
        Some("INDEX") => parse_create_index(parts),
        other => {
            return Err(ParseError::Expected {
                expected: "TABLE or INDEX".into(),
                found: other.map(|s| s.to_string()),
            });
        }
    }
}

fn parse_drop<'a, I>(parts: &mut std::iter::Peekable<I>) -> Result<Statement, ParseError>
where
    I: Iterator<Item = &'a str>,
{
    parts.next(); // DROP

    match parts.next() {
        Some("TABLE") => {
            let name = parts.next().ok_or(ParseError::UnexpectedEOF)?.to_string();

            Ok(Statement::DropTable(DropTableStmt { table_name: name }))
        }
        Some("INDEX") => {
            let name = parts.next().ok_or(ParseError::UnexpectedEOF)?.to_string();

            Ok(Statement::DropIndex { name })
        }
        other => {
            return Err(ParseError::Expected {
                expected: "TABLE or INDEX".into(),
                found: other.map(|s| s.to_string()),
            });
        }
    }
}

fn parse_create_index<'a, I>(parts: &mut std::iter::Peekable<I>) -> Result<Statement, ParseError>
where
    I: Iterator<Item = &'a str>,
{
    let name = parts.next().ok_or(ParseError::UnexpectedEOF)?.to_string();

    let on_token = parts.next();
    if on_token != Some("ON") {
        return Err(ParseError::Expected {
            expected: "ON".into(),
            found: on_token.map(|s| s.to_string()),
        });
    }

    let table = parts.next().ok_or(ParseError::UnexpectedEOF)?.to_string();
    let col = parts
        .next()
        .ok_or(ParseError::UnexpectedEOF)?
        .trim_start_matches('(')
        .trim_end_matches(')')
        .to_string();

    Ok(Statement::CreateIndex {
        name,
        table,
        column: col,
    })
}

fn parse_create_table<'a, I>(parts: &mut std::iter::Peekable<I>) -> Result<Statement, ParseError>
where
    I: Iterator<Item = &'a str>,
{
    let table_name = parts.next().ok_or(ParseError::UnexpectedEOF)?.to_string();

    let mut columns = Vec::new();

    // Expect opening "(" (may be attached to identifier)
    let first = parts.next().ok_or(ParseError::UnexpectedEOF)?;
    let mut token = first.trim_start_matches('(').to_string();

    loop {
        // Column name (strip trailing comma or paren)
        let col_name = token
            .trim_end_matches(',')
            .trim_end_matches(')')
            .to_string();

        // Column type
        let ty_token = parts.next().ok_or(ParseError::UnexpectedEOF)?;
        let ty_str = ty_token.trim_end_matches(',').trim_end_matches(')');

        let ty = match ty_str {
            "INT" | "INTEGER" => SqlType::Int,
            "BOOL" | "BOOLEAN" => SqlType::Bool,
            "TEXT" | "STRING" => SqlType::Text,
            other => {
                return Err(ParseError::Unsupported(other.into()));
            }
        };

        // Check if we hit the closing paren
        let mut has_close_paren = ty_token.ends_with(')');

        // Optional NULL / NOT NULL
        let mut nullable = true;
        if !has_close_paren {
            if let Some(&next) = parts.peek() {
                let next_clean = next.trim_end_matches(',').trim_end_matches(')');
                match next_clean {
                    "NOT" => {
                        parts.next(); // NOT
                        let null_token = parts.next().ok_or(ParseError::UnexpectedEOF)?;
                        if !null_token.starts_with("NULL") {
                            return Err(ParseError::Expected {
                                expected: "NULL".into(),
                                found: Some(null_token.to_string()),
                            });
                        }
                        nullable = false;
                        if null_token.ends_with(')') {
                            has_close_paren = true;
                        }
                    }
                    "NULL" => {
                        let null_token = parts.next().ok_or(ParseError::UnexpectedEOF)?;
                        nullable = true;
                        if null_token.ends_with(')') {
                            has_close_paren = true;
                        }
                    }
                    _ => {
                        if next.ends_with(')') {
                            has_close_paren = true;
                        }
                    }
                }
            }
        }

        columns.push(ColumnDef {
            name: col_name,
            ty,
            nullable,
        });

        // If we found closing paren, we're done
        if has_close_paren {
            break;
        }

        // Look for comma or next token
        match parts.peek() {
            Some(&tok) if tok.starts_with(')') || tok == ")" => {
                parts.next();
                break;
            }
            Some(&tok) if tok == "," => {
                parts.next(); // consume comma
                token = parts.next().ok_or(ParseError::UnexpectedEOF)?.to_string();
            }
            Some(_) => {
                token = parts.next().ok_or(ParseError::UnexpectedEOF)?.to_string();
            }
            None => break,
        }
    }

    Ok(Statement::CreateTable(CreateTableStmt {
        table_name,
        columns,
    }))
}

pub fn parse_select<'a, I>(parts: &mut std::iter::Peekable<I>) -> Result<Statement, ParseError>
where
    I: Iterator<Item = &'a str>,
{
    parts.next(); // SELECT

    let mut columns = Vec::new();
    while let Some(&tok) = parts.peek() {
        if tok == "FROM" {
            break;
        }
        let tok = parts.next().unwrap().trim_end_matches(',');
        columns.push(parse_column(tok));
    }

    if parts.next() != Some("FROM") {
        return Err(ParseError::Expected {
            expected: "FROM".into(),
            found: parts.peek().map(|s| s.to_string()),
        });
    }

    let mut from = parse_table(parts)?;

    while parts.peek() == Some(&"JOIN") {
        parts.next(); // JOIN
        let right = parse_table(parts)?;

        if parts.next() != Some("ON") {
            return Err(ParseError::Expected {
                expected: "ON".into(),
                found: parts.peek().map(|s| s.to_string()),
            });
        }

        let on = parse_simple_predicate(parts)?;

        from = FromItem::Join {
            left: Box::new(from),
            right: Box::new(right),
            on,
        };
    }

    let mut where_clause = None;
    let mut order_by = Vec::new();
    let mut limit = None;

    while let Some(tok) = parts.next() {
        match tok {
            "WHERE" => {
                let mut expr = parse_simple_predicate(parts)?;
                while parts.peek() == Some(&"AND") {
                    parts.next();
                    let rhs = parse_simple_predicate(parts)?;
                    expr = Expr::Binary {
                        left: Box::new(expr),
                        op: BinaryOp::And,
                        right: Box::new(rhs),
                    };
                }
                where_clause = Some(expr);
            }

            "ORDER" => {
                if parts.next() != Some("BY") {
                    return Err(ParseError::Expected {
                        expected: "BY".into(),
                        found: parts.peek().map(|s| s.to_string()),
                    });
                }

                loop {
                    let col = parts
                        .next()
                        .ok_or(ParseError::UnexpectedEOF)?
                        .trim_end_matches(',');
                    let mut asc = true;

                    if let Some(&"DESC") = parts.peek() {
                        parts.next();
                        asc = false;
                    } else if let Some(&"ASC") = parts.peek() {
                        parts.next();
                    }

                    order_by.push(OrderByExpr {
                        expr: parse_column(col),
                        asc,
                    });

                    if matches!(parts.peek(), Some(&"LIMIT") | None) {
                        break;
                    }
                }
            }

            "LIMIT" => {
                let limit_str = parts.next().ok_or(ParseError::UnexpectedEOF)?;
                let limit_val = limit_str
                    .parse::<usize>()
                    .map_err(|_| ParseError::InvalidLiteral(limit_str.to_string()))?;
                limit = Some(limit_val);
            }

            _ => {}
        }
    }

    Ok(Statement::Select(SelectStmt {
        columns,
        from,
        where_clause,
        order_by,
        limit,
    }))
}

fn parse_simple_predicate<'a, I>(parts: &mut std::iter::Peekable<I>) -> Result<Expr, ParseError>
where
    I: Iterator<Item = &'a str>,
{
    let left = parse_column(parts.next().ok_or(ParseError::UnexpectedEOF)?);

    let op_str = parts.next().ok_or(ParseError::UnexpectedEOF)?;
    let op = match op_str {
        "=" => BinaryOp::Eq,
        "!=" => BinaryOp::Neq,
        ">" => BinaryOp::Gt,
        ">=" => BinaryOp::Gte,
        "<" => BinaryOp::Lt,
        "<=" => BinaryOp::Lte,
        other => {
            return Err(ParseError::Unsupported(other.to_string()));
        }
    };

    let rhs = parts.next().ok_or(ParseError::UnexpectedEOF)?;

    // Handle string literals
    let right = if rhs.starts_with('\'') || rhs.starts_with('"') {
        let s = rhs.trim_matches('\'').trim_matches('"').to_string();
        Expr::Literal(Value::String(s))
    } else if let Ok(v) = rhs.parse::<i64>() {
        Expr::Literal(Value::Int64(v))
    } else if let Ok(b) = rhs.parse::<bool>() {
        Expr::Literal(Value::Bool(b))
    } else {
        parse_column(rhs)
    };

    Ok(Expr::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}

fn parse_table<'a, I>(parts: &mut std::iter::Peekable<I>) -> Result<FromItem, ParseError>
where
    I: Iterator<Item = &'a str>,
{
    let name = parts.next().ok_or(ParseError::UnexpectedEOF)?.to_string();

    let alias = match parts.peek() {
        Some(&tok)
            if tok != "JOIN"
                && tok != "ON"
                && tok != "WHERE"
                && tok != "ORDER"
                && tok != "LIMIT" =>
        {
            Some(parts.next().unwrap().to_string())
        }
        _ => None,
    };

    Ok(FromItem::Table { name, alias })
}

fn parse_column(tok: &str) -> Expr {
    if let Some((t, c)) = tok.split_once('.') {
        Expr::Column {
            table: Some(t.to_string()),
            name: c.to_string(),
        }
    } else {
        Expr::Column {
            table: None,
            name: tok.to_string(),
        }
    }
}
