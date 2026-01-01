use crate::exec::expr_eval::eval_predicate;
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
        self.input.open();
    }

    fn next(&mut self) -> Option<Row> {
        while let Some(row) = self.input.next() {
            let val = eval_predicate(&self.predicate, &row);
            println!("[FilterExec] row = {:?}, predicate = {:?}", row, val);

            if matches!(val, true) {
                println!("[FilterExec] PASSED");

                return Some(row);
            } else {
                println!("[FilterExec] REJECTED");
            }
        }

        println!("[FilterExec] EOF");
        None
    }
    fn close(&mut self) {
        self.input.close();
    }
}
