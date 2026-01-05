use crate::exec::evaluator::Evaluator;
use crate::exec::operator::{Operator, Row};
use crate::ir::expr::Expr;

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
        println!("FilterExec.open()");
        self.input.open();
    }

    fn next(&mut self) -> Option<Row> {
        println!("FilterExec.next()");
        while let Some(row) = self.input.next() {
            let ev = Evaluator::new(&row);
            let passed = ev.eval_predicate(&self.predicate);

            println!("[FilterExec] row = {:?}, predicate = {}", row, passed);

            if passed {
                println!("[FilterExec] PASSED");
                return Some(row);
            } else {
                println!("[FilterExec] REJECTED");
                continue;
            }
        }

        None
    }

    fn close(&mut self) {
        self.input.close();
    }
}
