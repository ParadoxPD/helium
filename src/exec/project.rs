use crate::common::value::Value;
use crate::exec::operator::{Operator, Row};
use crate::ir::expr::{BinaryOp, Expr, UnaryOp};

pub struct ProjectExec {
    input: Box<dyn Operator>,
    exprs: Vec<(Expr, String)>,
}

impl ProjectExec {
    pub fn new(input: Box<dyn Operator>, exprs: Vec<(Expr, String)>) -> Self {
        Self { input, exprs }
    }
}

impl Operator for ProjectExec {
    fn open(&mut self) {
        self.input.open();
    }

    fn next(&mut self) -> Option<Row> {
        let row = self.input.next()?;
        let mut out = Row::new();

        for (expr, alias) in &self.exprs {
            let value = eval_expr(expr, &row);
            out.insert(alias.clone(), value);
        }

        Some(out)
    }

    fn close(&mut self) {
        self.input.close();
    }
}

fn eval_expr(expr: &Expr, row: &Row) -> Value {
    match expr {
        Expr::Column(c) => row.get(&c.name).cloned().unwrap_or(Value::Null),

        Expr::Literal(v) => v.clone(),

        Expr::Unary { op, expr } => {
            let v = eval_expr(expr, row);
            match (op, v) {
                (UnaryOp::Neg, Value::Int64(x)) => Value::Int64(-x),
                (UnaryOp::Not, Value::Bool(b)) => Value::Bool(!b),
                _ => Value::Null,
            }
        }

        Expr::Binary { left, op, right } => {
            let l = eval_expr(left, row);
            let r = eval_expr(right, row);

            match (l, op, r) {
                (Value::Int64(a), BinaryOp::Add, Value::Int64(b)) => Value::Int64(a + b),
                (Value::Int64(a), BinaryOp::Sub, Value::Int64(b)) => Value::Int64(a - b),
                (Value::Int64(a), BinaryOp::Mul, Value::Int64(b)) => Value::Int64(a * b),
                (Value::Int64(a), BinaryOp::Eq, Value::Int64(b)) => Value::Bool(a == b),
                (Value::Int64(a), BinaryOp::Gt, Value::Int64(b)) => Value::Bool(a > b),
                (Value::Bool(a), BinaryOp::And, Value::Bool(b)) => Value::Bool(a && b),
                (Value::Bool(a), BinaryOp::Or, Value::Bool(b)) => Value::Bool(a || b),
                _ => Value::Null,
            }
        }

        Expr::Null => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::value::Value;
    use crate::exec::operator::Operator;
    use crate::exec::scan::ScanExec;
    use crate::ir::expr::{BinaryOp, Expr};

    fn row(pairs: &[(&str, Value)]) -> Row {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn project_selects_columns() {
        let data = vec![row(&[
            ("name", Value::String("Alice".into())),
            ("age", Value::Int64(30)),
        ])];

        let scan = ScanExec::new(data);

        let mut project =
            ProjectExec::new(Box::new(scan), vec![(Expr::col("name"), "name".into())]);

        project.open();
        let out = project.next().unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out.get("name"), Some(&Value::String("Alice".into())));
    }

    #[test]
    fn project_computes_expressions() {
        let data = vec![row(&[("age", Value::Int64(20))])];

        let scan = ScanExec::new(data);

        let expr = Expr::bin(Expr::col("age"), BinaryOp::Add, Expr::lit(Value::Int64(1)));

        let mut project = ProjectExec::new(Box::new(scan), vec![(expr, "next_age".into())]);

        project.open();
        let out = project.next().unwrap();

        assert_eq!(out.get("next_age"), Some(&Value::Int64(21)));
    }

    #[test]
    fn project_handles_missing_column_as_null() {
        let data = vec![row(&[("name", Value::String("Bob".into()))])];

        let scan = ScanExec::new(data);

        let mut project = ProjectExec::new(Box::new(scan), vec![(Expr::col("age"), "age".into())]);

        project.open();
        let out = project.next().unwrap();

        assert_eq!(out.get("age"), Some(&Value::Null));
    }

    #[test]
    fn project_preserves_row_count() {
        let data = vec![
            row(&[("x", Value::Int64(1))]),
            row(&[("x", Value::Int64(2))]),
        ];

        let scan = ScanExec::new(data);

        let mut project = ProjectExec::new(Box::new(scan), vec![(Expr::col("x"), "x".into())]);

        project.open();
        assert!(project.next().is_some());
        assert!(project.next().is_some());
        assert!(project.next().is_none());
    }
}
