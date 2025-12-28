use crate::common::value::Value;
use crate::exec::expr_eval::eval_predicate;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::value::Value;
    use crate::exec::operator::Operator;
    use crate::exec::scan::ScanExec;
    use crate::exec::test_util::qrow;
    use crate::ir::expr::{BinaryOp, Expr};

    #[test]
    fn filter_removes_rows() {
        let data = vec![
            qrow("t", &[("age", Value::Int64(10))]),
            qrow("t", &[("age", Value::Int64(30))]),
        ];

        let scan = ScanExec::new(data);
        let predicate = Expr::bin(
            Expr::bound_col("t", "age"),
            BinaryOp::Gt,
            Expr::lit(Value::Int64(18)),
        );

        let mut filter = FilterExec::new(Box::new(scan), predicate);
        filter.open();

        let row = filter.next().unwrap();
        assert_eq!(row.get("t.age"), Some(&Value::Int64(30)));
        assert!(filter.next().is_none());
    }
}
