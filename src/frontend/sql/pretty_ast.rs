use crate::frontend::sql::ast::*;

pub fn pretty_ast(stmt: &Statement) -> String {
    let mut out = String::new();
    pretty_stmt(stmt, 0, &mut out);
    out
}

fn indent(n: usize) -> String {
    "  ".repeat(n)
}

fn pretty_stmt(stmt: &Statement, depth: usize, out: &mut String) {
    match stmt {
        Statement::Select(s) => {
            out.push_str(&format!("{}Select\n", indent(depth)));
            pretty_select(s, depth + 1, out);
        }

        Statement::Explain { analyze, stmt } => {
            out.push_str(&format!("{}Explain analyze={}\n", indent(depth), analyze));
            pretty_stmt(stmt, depth + 1, out);
        }

        Statement::CreateIndex {
            name,
            table,
            column,
        } => {
            out.push_str(&format!(
                "{}CreateIndex {} ON {}({})\n",
                indent(depth),
                name,
                table,
                column
            ));
        }

        Statement::DropIndex { name } => {
            out.push_str(&format!("{}DropIndex {}\n", indent(depth), name));
        }

        other => println!("{:?}", other),
    }
}

fn pretty_select(s: &SelectStmt, depth: usize, out: &mut String) {
    out.push_str(&format!("{}Columns\n", indent(depth)));
    for c in &s.columns {
        out.push_str(&format!("{}- {:?}\n", indent(depth + 1), c));
    }

    out.push_str(&format!("{}From\n", indent(depth)));
    pretty_from(&s.from, depth + 1, out);

    if let Some(w) = &s.where_clause {
        out.push_str(&format!("{}Where\n", indent(depth)));
        pretty_expr(w, depth + 1, out);
    }

    if !s.order_by.is_empty() {
        out.push_str(&format!("{}OrderBy\n", indent(depth)));
        for o in &s.order_by {
            out.push_str(&format!(
                "{}- {:?} {}\n",
                indent(depth + 1),
                o.expr,
                if o.asc { "ASC" } else { "DESC" }
            ));
        }
    }

    if let Some(l) = s.limit {
        out.push_str(&format!("{}Limit {}\n", indent(depth), l));
    }
}

fn pretty_from(from: &FromItem, depth: usize, out: &mut String) {
    match from {
        FromItem::Table { name, alias } => {
            out.push_str(&format!(
                "{}Table {}{}\n",
                indent(depth),
                name,
                alias
                    .as_ref()
                    .map(|a| format!(" AS {}", a))
                    .unwrap_or_default()
            ));
        }

        FromItem::Join { left, right, on } => {
            out.push_str(&format!("{}Join\n", indent(depth)));
            pretty_from(left, depth + 1, out);
            pretty_from(right, depth + 1, out);
            out.push_str(&format!("{}On\n", indent(depth + 1)));
            pretty_expr(on, depth + 2, out);
        }
    }
}

fn pretty_expr(e: &Expr, depth: usize, out: &mut String) {
    match e {
        Expr::Column { table, name } => {
            out.push_str(&format!(
                "{}Table {:?} Column {}\n",
                indent(depth),
                table,
                name
            ));
        }
        Expr::Literal(v) => {
            out.push_str(&format!("{}Literal {}\n", indent(depth), v));
        }
        Expr::Binary { left, op, right } => {
            out.push_str(&format!("{}Binary {:?}\n", indent(depth), op));
            pretty_expr(left, depth + 1, out);
            pretty_expr(right, depth + 1, out);
        }
        Expr::Unary { op, expr } => {
            out.push_str(&format!("{}Unary {:?}\n", indent(depth), op));
            pretty_expr(expr, depth + 1, out);
        }
    }
}
