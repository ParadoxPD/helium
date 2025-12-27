use super::ast::*;
use std::str::FromStr;

pub fn parse(sql: &str) -> SelectStmt {
    let sql = sql.trim().trim_end_matches(';');
    let mut parts = sql.split_whitespace().peekable();

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
    let table = parts.next().unwrap().to_string();

    let mut where_clause = None;
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
            "LIMIT" => {
                limit = Some(usize::from_str(parts.next().unwrap()).unwrap());
            }
            _ => {}
        }
    }

    SelectStmt {
        columns,
        table,
        where_clause,
        limit,
    }
}

fn parse_simple_predicate<'a, I>(parts: &mut std::iter::Peekable<I>) -> Expr
where
    I: Iterator<Item = &'a str>,
{
    let left = parts.next().expect("lhs");
    let op = parts.next().expect("op");
    let right = parts.next().expect("rhs");

    let bin_op = match op {
        "=" => BinaryOp::Eq,
        ">" => BinaryOp::Gt,
        "<" => BinaryOp::Lt,
        _ => panic!("unsupported operator"),
    };

    Expr::Binary {
        left: Box::new(Expr::Column(left.into())),
        op: bin_op,
        right: Box::new(Expr::LiteralInt(right.parse().unwrap())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_predicates() {
        let sql = "SELECT name FROM users WHERE age > 18 AND score > 50;";
        let stmt = parse(sql);

        assert!(stmt.where_clause.is_some());
    }
}
