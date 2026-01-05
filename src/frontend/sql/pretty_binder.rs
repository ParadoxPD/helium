use crate::{
    frontend::sql::binder::{BoundFromItem, BoundSelect},
    ir::expr::Expr as IRExpr,
};

pub fn pretty_bound(b: &BoundSelect) -> String {
    let mut out = String::new();
    out.push_str("BoundSelect\n");

    out.push_str("  Columns\n");
    for c in &b.columns {
        pretty_ir(&c.0, 2, &mut out);
    }

    out.push_str("  From\n");
    pretty_from(&b.from, 2, &mut out);

    if let Some(w) = &b.where_clause {
        out.push_str("  Where\n");
        pretty_ir(w, 2, &mut out);
    }

    out
}

fn indent(n: usize) -> String {
    "  ".repeat(n)
}

fn pretty_from(f: &BoundFromItem, depth: usize, out: &mut String) {
    match f {
        BoundFromItem::Table { name, alias } => {
            out.push_str(&format!("{}Table {} AS {}\n", indent(depth), name, alias));
        }
        BoundFromItem::Join { left, right, on } => {
            out.push_str(&format!("{}Join\n", indent(depth)));
            pretty_from(left, depth + 1, out);
            pretty_from(right, depth + 1, out);
            out.push_str(&format!("{}On\n", indent(depth + 1)));
            pretty_ir(on, depth + 2, out);
        }
    }
}

fn pretty_ir(e: &IRExpr, depth: usize, out: &mut String) {
    match e {
        IRExpr::BoundColumn { table, name } => {
            out.push_str(&format!("{}Column {}.{}\n", indent(depth), table, name));
        }
        IRExpr::Literal(v) => {
            out.push_str(&format!("{}Literal {:?}\n", indent(depth), v));
        }
        IRExpr::Binary { left, op, right } => {
            out.push_str(&format!("{}Binary {:?}\n", indent(depth), op));
            pretty_ir(left, depth + 1, out);
            pretty_ir(right, depth + 1, out);
        }
        other => println!("{:?}", other),
    }
}
