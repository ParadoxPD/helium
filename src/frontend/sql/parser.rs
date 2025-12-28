use super::ast::*;
use std::str::FromStr;

pub fn parse(sql: &str) -> Statement {
    let sql = sql.trim().trim_end_matches(';');
    let mut parts = sql.split_whitespace().peekable();

    if parts.peek() == Some(&"EXPLAIN") {
        parts.next(); // consume EXPLAIN

        let analyze = if parts.peek() == Some(&"ANALYZE") {
            parts.next();
            true
        } else {
            false
        };

        let rest: String = parts.collect::<Vec<_>>().join(" ");
        return Statement::Explain {
            analyze,
            stmt: Box::new(parse(&rest)),
        };
    }

    Statement::Select(parse_select(&mut parts))
}

fn parse_table<'a, I>(parts: &mut std::iter::Peekable<I>) -> FromItem
where
    I: Iterator<Item = &'a str>,
{
    let name = parts.next().expect("table name").to_string();

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

    FromItem::Table { name, alias }
}

pub fn parse_select<'a, I>(mut parts: &mut std::iter::Peekable<I>) -> SelectStmt
where
    I: Iterator<Item = &'a str>,
{
    assert_eq!(parts.next(), Some("SELECT"));

    let mut columns = Vec::new();
    while let Some(&tok) = parts.peek() {
        if tok == "FROM" {
            break;
        }
        let col = parts.next().unwrap().trim_end_matches(',');
        columns.push(col.to_string());
    }

    assert_eq!(parts.next(), Some("FROM"));
    let mut from = parse_table(&mut parts);

    // Handle JOIN chains
    while parts.peek() == Some(&"JOIN") {
        parts.next(); // consume JOIN

        let right = parse_table(&mut parts);

        assert_eq!(parts.next(), Some("ON"));

        let on = parse_simple_predicate(&mut parts);

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
                let mut expr = parse_simple_predicate(&mut parts);

                while let Some(&"AND") = parts.peek() {
                    parts.next();
                    let rhs = parse_simple_predicate(&mut parts);

                    expr = Expr::Binary {
                        left: Box::new(expr),
                        op: BinaryOp::And,
                        right: Box::new(rhs),
                    };
                }
                where_clause = Some(expr);
            }
            "ORDER" => {
                assert_eq!(parts.next(), Some("BY"));

                loop {
                    let mut col = parts.next().expect("column").to_string();
                    let mut asc = true;

                    // Normalize column token
                    let col_has_comma = col.ends_with(',');
                    if col_has_comma {
                        col.pop();
                    }

                    // Lookahead for ASC/DESC
                    if let Some(&next) = parts.peek() {
                        let mut dir = next.to_string();
                        let dir_has_comma = dir.ends_with(',');
                        if dir_has_comma {
                            dir.pop();
                        }

                        if dir == "ASC" {
                            parts.next();
                            asc = true;
                        } else if dir == "DESC" {
                            parts.next();
                            asc = false;
                        }
                    }

                    order_by.push(OrderByExpr { column: col, asc });

                    // Stop if next clause begins
                    match parts.peek() {
                        Some(&"LIMIT") | Some(&"WHERE") | None => break,
                        _ => {}
                    }
                }
            }

            "LIMIT" => {
                limit = Some(usize::from_str(parts.next().unwrap()).unwrap());
            }
            _ => {}
        }
    }

    SelectStmt {
        columns,
        from,
        where_clause,
        order_by,
        limit,
    }
}

fn parse_simple_predicate<'a, I>(parts: &mut std::iter::Peekable<I>) -> Expr
where
    I: Iterator<Item = &'a str>,
{
    let left = Expr::Column(parts.next().unwrap().to_string());

    let op = match parts.next().unwrap() {
        "=" => BinaryOp::Eq,
        ">" => BinaryOp::Gt,
        "<" => BinaryOp::Lt,
        _ => panic!("unsupported operator"),
    };

    let rhs_tok = parts.next().unwrap();

    let right = if let Ok(v) = rhs_tok.parse::<i64>() {
        Expr::LiteralInt(v)
    } else {
        Expr::Column(rhs_tok.to_string())
    };

    Expr::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_predicates() {
        let sql = "SELECT name FROM users WHERE age > 18 AND score > 50;";
        let stmt = parse(sql);
        match stmt {
            Statement::Select(select) => {
                assert!(select.where_clause.is_some());
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parses_order_by() {
        let stmt = parse("SELECT name FROM users ORDER BY age DESC, name ASC;");
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.order_by.len(), 2);
                assert!(!s.order_by[0].asc);
                assert!(s.order_by[1].asc);
            }
            _ => panic!("expected select"),
        }
    }
}

#[cfg(test)]
mod join_tests {
    use super::*;
    use crate::frontend::sql::ast::{FromItem, Statement};

    #[test]
    fn parses_simple_join() {
        let stmt = parse("SELECT name FROM users u JOIN orders o ON u.id = o.user_id;");

        match stmt {
            Statement::Select(s) => match s.from {
                FromItem::Join { left, right, .. } => {
                    assert!(matches!(*left, FromItem::Table { .. }));
                    assert!(matches!(*right, FromItem::Table { .. }));
                }
                _ => panic!("expected join"),
            },
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parses_chained_joins() {
        let stmt = parse(
            "SELECT name FROM users u \
             JOIN orders o ON u.id = o.user_id \
             JOIN payments p ON o.id = p.order_id;",
        );

        match stmt {
            Statement::Select(s) => match s.from {
                FromItem::Join { left, .. } => {
                    // left itself should be another join
                    assert!(matches!(*left, FromItem::Join { .. }));
                }
                _ => panic!("expected join"),
            },
            _ => panic!("expected select"),
        }
    }
}
