use std::cmp::Ordering;

use crate::exec::evaluator::Evaluator;
use crate::exec::operator::{Operator, Row};
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

fn compare_rows(a: &Row, b: &Row, keys: &[(Expr, bool)]) -> Ordering {
    let eva = Evaluator::new(a);
    let evb = Evaluator::new(b);

    for (expr, asc) in keys {
        let va = eva.eval_expr(expr);
        let vb = evb.eval_expr(expr);

        let ord = match (va, vb) {
            // NULL = NULL → equal
            (None, None) => Ordering::Equal,

            // NULLS LAST (default SQL behavior)
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,

            // Both non-NULL → compare values
            (Some(x), Some(y)) => x.cmp(&y).expect("Error"),
        };

        if ord != Ordering::Equal {
            return if *asc { ord } else { ord.reverse() };
        }
    }

    Ordering::Equal
}
