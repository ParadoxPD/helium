
use crate::exec::evaluator::Evaluator;
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
        }
    }

    fn merge_rows(left: &Row, right: &Row) -> Row {
        let mut out = left.clone();
        for (k, v) in right.values.clone() {
            out.values.insert(k, v);
        }
        out
    }
}

impl Operator for JoinExec {
    fn open(&mut self) {
        // Always reset state
        self.left_rows.clear();
        self.right_rows.clear();
        self.i = 0;
        self.j = 0;

        self.left.open();
        self.right.open();
        while let Some(row) = self.left.next() {
            self.left_rows.push(row);
        }

        while let Some(row) = self.right.next() {
            self.right_rows.push(row);
        }
        println!("LEFT = {:?}", self.left_rows.len());
        println!("RIGHT = {:?}", self.right_rows.len());
    }

    fn next(&mut self) -> Option<Row> {
        while self.i < self.left_rows.len() {
            while self.j < self.right_rows.len() {
                let l = &self.left_rows[self.i];
                let r = &self.right_rows[self.j];
                self.j += 1;

                let merged = Self::merge_rows(l, r);
                let ev = Evaluator::new(&merged);
                println!("JOIN ROW KEYS = {:?}", merged.values.keys());

                if ev.eval_predicate(&self.on) {
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
    }
}
