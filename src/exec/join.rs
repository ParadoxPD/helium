use std::sync::Mutex;

use crate::buffer::buffer_pool::BufferPool;
use crate::exec::expr_eval::eval_predicate;
use crate::exec::operator::{Operator, Row};
use crate::ir::expr::Expr;
use crate::storage::page_manager::FilePageManager;

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
        for (k, v) in right.values.clone() {
            out.values.insert(k, v);
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
