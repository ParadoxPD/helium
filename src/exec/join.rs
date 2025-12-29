use crate::exec::expr_eval::eval_predicate;
use crate::exec::operator::{Operator, Row};
use crate::ir::expr::Expr;

pub struct JoinExec {
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,
    on: Expr,

    left_rows: Vec<Row>,
    right_rows: Vec<Row>,

    i: usize,
    j: usize,

    opened: bool,
}

impl JoinExec {
    pub fn new(left: Box<dyn Operator>, right: Box<dyn Operator>, on: Expr) -> Self {
        Self {
            left,
            right,
            on,
            left_rows: Vec::new(),
            right_rows: Vec::new(),
            i: 0,
            j: 0,
            opened: false,
        }
    }

    fn merge_rows(left: &Row, right: &Row) -> Row {
        let mut out = left.clone();
        for (k, v) in right {
            out.insert(k.clone(), v.clone());
        }
        out
    }
}

impl Operator for JoinExec {
    fn open(&mut self) {
        if self.opened {
            return;
        }
        self.opened = true;

        self.left.open();
        self.right.open();

        // Materialize both sides (simple, correct)
        while let Some(row) = self.left.next() {
            self.left_rows.push(row);
        }

        while let Some(row) = self.right.next() {
            self.right_rows.push(row);
        }

        self.i = 0;
        self.j = 0;
    }

    fn next(&mut self) -> Option<Row> {
        while self.i < self.left_rows.len() {
            while self.j < self.right_rows.len() {
                let l = &self.left_rows[self.i];
                let r = &self.right_rows[self.j];
                self.j += 1;

                let merged = Self::merge_rows(l, r);
                if eval_predicate(&self.on, &merged) {
                    println!("JOIN ROW = {:?}", merged);

                    return Some(merged);
                }
            }

            self.j = 0;
            self.i += 1;
        }

        None
    }

    fn close(&mut self) {
        self.left.close();
        self.right.close();
        self.left_rows.clear();
        self.right_rows.clear();
        self.opened = false;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::common::value::Value;
    use crate::exec::operator::Operator;
    use crate::exec::scan::ScanExec;
    use crate::exec::test_util::qrow;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::storage::in_memory::InMemoryTable;

    #[test]
    fn join_matches_rows() {
        let left_data = vec![
            qrow(
                "u",
                &[
                    ("id", Value::Int64(1)),
                    ("name", Value::String("Alice".into())),
                ],
            ),
            qrow(
                "u",
                &[
                    ("id", Value::Int64(2)),
                    ("name", Value::String("Bob".into())),
                ],
            ),
        ];

        let right_data = vec![
            qrow(
                "o",
                &[("user_id", Value::Int64(1)), ("amount", Value::Int64(200))],
            ),
            qrow(
                "o",
                &[("user_id", Value::Int64(3)), ("amount", Value::Int64(50))],
            ),
        ];

        let left = ScanExec::new(Arc::new(InMemoryTable::new("u".into(), left_data)));
        let right = ScanExec::new(Arc::new(InMemoryTable::new("o".into(), right_data)));

        let on = Expr::bin(
            Expr::bound_col("u", "id"),
            BinaryOp::Eq,
            Expr::bound_col("o", "user_id"),
        );

        let mut join = JoinExec::new(Box::new(left), Box::new(right), on);
        join.open();

        let result = join.next().unwrap();
        assert_eq!(result.get("u.name"), Some(&Value::String("Alice".into())));
        assert_eq!(result.get("o.amount"), Some(&Value::Int64(200)));

        assert!(join.next().is_none());
    }
}
