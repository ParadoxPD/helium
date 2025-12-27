use crate::common::value::Value;
use crate::exec::operator::{Operator, Row};
use crate::ir::expr::{BinaryOp, Expr};

pub struct FilterExec {
    input: Box<dyn Operator>,
    predicate: Expr,
}

impl FilterExec {
    pub fn new(input: Box<dyn Operator>, predicate: Expr) -> Self {
        Self { input, predicate }
    }
}

impl Operator for FilterExec {
    fn open(&mut self) {
        self.input.open();
    }

    fn next(&mut self) -> Option<Row> {
        while let Some(row) = self.input.next() {
            if eval_predicate(&self.predicate, &row) {
                return Some(row);
            }
        }
        None
    }

    fn close(&mut self) {
        self.input.close();
    }
}

fn eval_predicate(expr: &Expr, row: &Row) -> bool {
    match expr {
        Expr::Binary { left, op, right } => {
            let l = eval_expr(left, row);
            let r = eval_expr(right, row);

            match (l, op, r) {
                (Value::Int64(a), BinaryOp::Gt, Value::Int64(b)) => a > b,
                (Value::Int64(a), BinaryOp::Eq, Value::Int64(b)) => a == b,
                (Value::Bool(a), BinaryOp::And, Value::Bool(b)) => a && b,
                _ => false,
            }
        }
        _ => false,
    }
}

fn eval_expr(expr: &Expr, row: &Row) -> Value {
    match expr {
        Expr::Column(c) => row.get(&c.name).cloned().unwrap_or(Value::Null),
        Expr::Literal(v) => v.clone(),
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::value::Value;
    use crate::exec::operator::Operator;
    use crate::exec::scan::ScanExec;
    use crate::ir::expr::{BinaryOp, Expr};

    #[test]
    fn filter_removes_rows() {
        let data = vec![
            [("age", Value::Int64(10))]
                .into_iter()
                .map(|(k, v)| (k.into(), v))
                .collect(),
            [("age", Value::Int64(30))]
                .into_iter()
                .map(|(k, v)| (k.into(), v))
                .collect(),
        ];

        let scan = ScanExec::new(data);
        let predicate = Expr::bin(Expr::col("age"), BinaryOp::Gt, Expr::lit(Value::Int64(18)));

        let mut filter = FilterExec::new(Box::new(scan), predicate);
        filter.open();

        let row = filter.next().unwrap();
        assert_eq!(row.get("age"), Some(&Value::Int64(30)));
        assert!(filter.next().is_none());
    }
}
