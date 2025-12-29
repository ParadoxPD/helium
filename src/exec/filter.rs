use crate::common::value::Value;
use crate::exec::expr_eval::{eval_predicate, eval_value};
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
            let val = eval_predicate(&self.predicate, &row);
            eprintln!("[FilterExec] row = {:?}, predicate = {:?}", row, val);

            if matches!(val, true) {
                eprintln!("[FilterExec] PASSED");
                return Some(row);
            } else {
                eprintln!("[FilterExec] REJECTED");
            }
        }

        eprintln!("[FilterExec] EOF");
        None
    }
    fn close(&mut self) {
        self.input.close();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::common::value::Value;
    use crate::exec::Catalog;
    use crate::exec::alias::AliasExec;
    use crate::exec::operator::Operator;
    use crate::exec::scan::ScanExec;
    use crate::exec::test_util::qrow;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::storage::in_memory::InMemoryTable;

    #[test]
    fn filter_removes_rows() {
        let data = vec![
            qrow("users", &[("age", Value::Int64(10))]),
            qrow("users", &[("age", Value::Int64(30))]),
        ];

        let table = InMemoryTable::new("users".into(), data);
        let scan = ScanExec::new(Arc::new(table));

        let aliased = AliasExec::new(
            Box::new(scan),
            "users".into(),
            "users".into(), // alias = table name
        );

        let predicate = Expr::bin(
            Expr::bound_col("users", "age"),
            BinaryOp::Gt,
            Expr::lit(Value::Int64(18)),
        );

        let mut filter = FilterExec::new(Box::new(aliased), predicate);
        filter.open();

        let row = filter.next().unwrap();
        assert_eq!(row.get("users.age"), Some(&Value::Int64(30)));
        assert!(filter.next().is_none());
    }
}
