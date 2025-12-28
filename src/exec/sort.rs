use crate::common::value::Value;
use crate::exec::expr_eval::eval_value;
use crate::exec::operator::{Operator, Row};
use crate::exec::scan::ScanExec;
use crate::ir::expr::Expr;

pub struct SortExec {
    input: Box<dyn Operator>,
    keys: Vec<(Expr, bool)>,
    rows: Vec<Row>,
    idx: usize,
}

impl SortExec {
    pub fn new(input: Box<dyn Operator>, keys: Vec<(Expr, bool)>) -> Self {
        Self {
            input,
            keys,
            rows: Vec::new(),
            idx: 0,
        }
    }
}

impl Operator for SortExec {
    fn open(&mut self) {
        self.rows.clear();
        self.idx = 0;
        self.input.open();

        while let Some(row) = self.input.next() {
            self.rows.push(row);
        }

        self.rows.sort_by(|a, b| compare_rows(a, b, &self.keys));
    }

    fn next(&mut self) -> Option<Row> {
        if self.idx >= self.rows.len() {
            None
        } else {
            let row = self.rows[self.idx].clone();
            self.idx += 1;
            Some(row)
        }
    }

    fn close(&mut self) {
        self.input.close();
    }
}

fn compare_rows(a: &Row, b: &Row, keys: &[(Expr, bool)]) -> std::cmp::Ordering {
    for (expr, asc) in keys {
        let va = eval_value(expr, a);
        let vb = eval_value(expr, b);

        let ord = match (va, vb) {
            (Value::Int64(x), Value::Int64(y)) => x.cmp(&y),
            (Value::String(x), Value::String(y)) => x.cmp(&y),
            _ => std::cmp::Ordering::Equal,
        };

        if ord != std::cmp::Ordering::Equal {
            return if *asc { ord } else { ord.reverse() };
        }
    }
    std::cmp::Ordering::Equal
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::value::Value;
    use crate::exec::operator::Row;
    use crate::exec::scan::ScanExec;
    use crate::exec::test_util::qrow;
    use crate::ir::expr::Expr;

    #[test]
    fn sort_orders_rows() {
        let data = vec![
            qrow("t", &[("age", Value::Int64(30))]),
            qrow("t", &[("age", Value::Int64(10))]),
        ];

        let scan = ScanExec::new(data);
        let mut sort = SortExec::new(Box::new(scan), vec![(Expr::bound_col("t", "age"), true)]);

        sort.open();
        assert_eq!(sort.next().unwrap().get("t.age"), Some(&Value::Int64(10)));
    }
}
